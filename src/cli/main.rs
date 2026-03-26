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
    },
    Download {
        hash: String,
        #[arg(short, long)]
        peer: String,
        #[arg(short, long)]
        output: String,
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
        Cli::Seed { path, listen } => {
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

            seeder.listen(&listen).await?;
        }
        Cli::Download { hash, peer, output } => {
            let info_hash = parse_hex_hash(&hash)?;
            let peer_id = generate_peer_id();
            let leecher = Leecher::new(info_hash, peer_id, output);

            println!("Connecting to {peer}...");
            let result = leecher.download(&peer).await?;
            println!(
                "Downloaded {} ({} pieces verified)",
                format_size(result.bytes_downloaded),
                result.pieces_verified,
            );
        }
    }

    Ok(())
}
