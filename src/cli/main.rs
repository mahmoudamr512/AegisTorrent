mod dashboard;

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;

use aegistorrent::network::leecher::Leecher;
use aegistorrent::network::seeder::Seeder;

#[derive(Parser)]
#[command(name = "aegis", version, about = "AegisTorrent P2P file distribution")]
enum Cli {
    Seed {
        path: String,
        #[arg(short, long, default_value = "127.0.0.1:6881")]
        listen: String,
        #[arg(long)]
        bootstrap: Vec<String>,
        #[arg(long, default_value = "6882")]
        dht_port: u16,
        #[arg(long)]
        no_dht: bool,
    },
    Download {
        hash: String,
        #[arg(short, long)]
        peer: Vec<String>,
        #[arg(short, long)]
        output: String,
        #[arg(long, default_value = "262144")]
        piece_size: usize,
        #[arg(long, default_value = "0")]
        total_size: u64,
        #[arg(long, default_value = "9090")]
        stats_port: u16,
        #[arg(long)]
        bootstrap: Vec<String>,
        #[arg(long, default_value = "6882")]
        dht_port: u16,
        #[arg(long)]
        no_dht: bool,
    },
}

fn generate_peer_id() -> [u8; 20] {
    let prefix = b"-AT0100-";
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let ts_bytes = ts.to_le_bytes();

    let mut id = [0u8; 20];
    id[..8].copy_from_slice(prefix);
    id[8..].copy_from_slice(&ts_bytes[..12]);
    id
}

fn parse_hex_hash(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut hash = [0u8; 32];
    for i in 0..32 {
        hash[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|e| format!("invalid hex at position {}: {e}", i * 2))?;
    }
    Ok(hash)
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.2} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli {
        Cli::Seed {
            path,
            listen,
            bootstrap,
            dht_port,
            no_dht,
        } => {
            let peer_id = generate_peer_id();
            let seeder = Seeder::from_file(Path::new(&path), peer_id)?;

            println!(
                "Seeding {} ({}, {} pieces, {} each)",
                path,
                format_size(seeder.total_size()),
                seeder.piece_count(),
                format_size(seeder.piece_size() as u64),
            );

            let hash = seeder.info_hash();
            let hex_str: String = hash.iter().map(|b| format!("{b:02x}")).collect();
            println!("Info hash: {hex_str}");
            println!("Listening on {listen}");

            if !no_dht && !bootstrap.is_empty() {
                use aegistorrent::network::discovery::dht::{DhtConfig, DhtNode};
                use aegistorrent::network::discovery::routing::NodeId;
                use aegistorrent::network::discovery::rpc::DhtMessage;
                use std::net::SocketAddr;

                let node_id: NodeId = peer_id[..20].try_into().unwrap();
                let dht_listen = format!("0.0.0.0:{dht_port}");
                let (dht_tx, _dht_rx) = tokio::sync::mpsc::channel(64);

                let dht_config = DhtConfig {
                    node_id,
                    listen_addr: dht_listen,
                    bootstrap_addrs: bootstrap.clone(),
                };
                let dht_node = DhtNode::new(dht_config, dht_tx);
                let info_hash = hash;
                let listen_port: u16 = listen
                    .rsplit(':')
                    .next()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(6881);

                println!("DHT: announcing on {} bootstrap node(s)", bootstrap.len());
                tokio::spawn(async move {
                    let socket = match tokio::net::UdpSocket::bind("0.0.0.0:0").await {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    let msg = DhtMessage::AnnouncePeer {
                        sender: node_id,
                        info_hash,
                        peer_port: listen_port,
                        score: 1.0,
                    };
                    let encoded = msg.encode();
                    loop {
                        for addr_str in &bootstrap {
                            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                                let _ = socket.send_to(&encoded, addr).await;
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                    }
                });
                tokio::spawn(dht_node.run());
            }

            seeder.listen(&listen).await?;
        }
        Cli::Download {
            hash,
            peer: peers,
            output,
            piece_size,
            total_size,
            stats_port,
            bootstrap,
            dht_port,
            no_dht,
        } => {
            let info_hash = parse_hex_hash(&hash)?;
            let peer_id = generate_peer_id();

            let mut all_peers = peers.clone();

            if !no_dht && !bootstrap.is_empty() {
                use aegistorrent::network::discovery::dht::{DhtConfig, DhtEvent, DhtNode};
                use aegistorrent::network::discovery::routing::{NodeId, NodeInfo};

                let node_id: NodeId = peer_id[..20].try_into().unwrap();
                let dht_listen = format!("0.0.0.0:{dht_port}");
                let (dht_tx, mut dht_rx) = tokio::sync::mpsc::channel(64);

                let initial_nodes: Vec<NodeInfo> = bootstrap
                    .iter()
                    .enumerate()
                    .map(|(i, addr)| {
                        let mut id = [0u8; 20];
                        id[0] = i as u8;
                        NodeInfo {
                            id,
                            addr: addr.clone(),
                        }
                    })
                    .collect();

                println!(
                    "DHT: looking up peers from {} bootstrap node(s)...",
                    bootstrap.len()
                );

                let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
                DhtNode::find_peers(&socket, node_id, info_hash, initial_nodes, dht_tx.clone())
                    .await;
                drop(socket);

                while let Ok(DhtEvent::PeersFound { peers: found, .. }) = dht_rx.try_recv() {
                    for entry in found {
                        if !all_peers.contains(&entry.addr) {
                            println!("DHT: found peer {}", entry.addr);
                            all_peers.push(entry.addr);
                        }
                    }
                }

                let dht_config = DhtConfig {
                    node_id,
                    listen_addr: dht_listen,
                    bootstrap_addrs: bootstrap,
                };
                let dht_node = DhtNode::new(dht_config, dht_tx);
                tokio::spawn(dht_node.run());
            }

            if all_peers.is_empty() {
                eprintln!("Error: no peers found (specify --peer or use --bootstrap for DHT)");
                std::process::exit(1);
            }

            if total_size == 0 {
                let leecher = Leecher::new(info_hash, peer_id, output);
                println!("Connecting to {}...", all_peers[0]);
                let result = leecher.download(&all_peers[0]).await?;
                println!(
                    "Downloaded {} ({} pieces verified)",
                    format_size(result.bytes_downloaded),
                    result.pieces_verified,
                );
            } else {
                use std::sync::{Arc, Mutex};

                use aegistorrent::network::coordinator::{DownloadCoordinator, DownloadProgress};

                let piece_count = (total_size as usize).div_ceil(piece_size) as u32;
                let progress = Arc::new(Mutex::new(DownloadProgress::new(piece_count, total_size)));

                println!("Connecting to {} peer(s)...", all_peers.len());

                let coordinator = DownloadCoordinator::new(
                    info_hash,
                    peer_id,
                    piece_size,
                    total_size,
                    std::path::PathBuf::from(&output),
                    all_peers,
                );

                let progress_for_download = Arc::clone(&progress);
                let download_handle =
                    tokio::spawn(async move { coordinator.run(Some(progress_for_download)).await });

                let stats_addr = format!("127.0.0.1:{stats_port}");
                let stats_progress = Arc::clone(&progress);
                tokio::spawn(async move {
                    aegistorrent::observability::stats_server::run_stats_server(
                        &stats_addr,
                        stats_progress,
                    )
                    .await;
                });

                dashboard::run_dashboard(progress).await;

                let result = download_handle
                    .await?
                    .map_err(|e| -> Box<dyn std::error::Error> { e })?;
                println!(
                    "\nDownloaded {} ({} pieces verified, {} peers used)",
                    format_size(result.bytes_downloaded),
                    result.pieces_verified,
                    result.peers_used,
                );
            }
        }
    }

    Ok(())
}
