use tempfile::TempDir;
use tokio::net::TcpListener;

use aegistorrent::network::leecher::Leecher;
use aegistorrent::network::seeder::Seeder;

#[tokio::test]
async fn two_peer_file_transfer() {
    let tmp = TempDir::new().unwrap();
    let input_path = tmp.path().join("input.bin");
    let output_path = tmp.path().join("output.bin");

    // 8 pieces * 256KB = 2MB, ensures piece_count is a multiple of 8
    // so bitfield byte count * 8 == actual piece count
    let size = 8 * 256 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    std::fs::write(&input_path, &data).unwrap();

    let seeder_id = *b"seeder-peer-id-12345";
    let leecher_id = *b"leecher-peer-id-1234";

    let seeder = Seeder::from_file(&input_path, seeder_id).unwrap();
    let info_hash = seeder.info_hash();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let seeder_handle = tokio::spawn(async move {
        let _ = seeder.listen_on(listener).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let leecher = Leecher::new(
        info_hash,
        leecher_id,
        output_path.to_str().unwrap().to_string(),
    );
    let result = leecher.download(&addr).await.unwrap();

    let downloaded = std::fs::read(&output_path).unwrap();
    assert_eq!(downloaded.len(), data.len());
    assert_eq!(downloaded, data);
    assert!(result.pieces_verified > 0);
    assert_eq!(result.bytes_downloaded, data.len() as u64);

    seeder_handle.abort();
}

#[tokio::test]
async fn multi_peer_download() {
    let tmp = TempDir::new().unwrap();
    let input_path = tmp.path().join("input.bin");
    let output_path = tmp.path().join("output.bin");

    let size = 8 * 256 * 1024; // 2MB, 8 pieces
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    std::fs::write(&input_path, &data).unwrap();

    let seeder1_id = *b"seeder1-peer-id-1234";
    let seeder2_id = *b"seeder2-peer-id-1234";
    let leecher_id = *b"leecher-peer-id-mp12";

    let seeder1 = Seeder::from_file(&input_path, seeder1_id).unwrap();
    let seeder2 = Seeder::from_file(&input_path, seeder2_id).unwrap();
    let info_hash = seeder1.info_hash();

    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap().to_string();

    let h1 = tokio::spawn(async move {
        let _ = seeder1.listen_on(listener1).await;
    });
    let h2 = tokio::spawn(async move {
        let _ = seeder2.listen_on(listener2).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    use aegistorrent::network::coordinator::DownloadCoordinator;

    let coordinator = DownloadCoordinator::new(
        info_hash,
        leecher_id,
        256 * 1024,
        size as u64,
        output_path.clone(),
        vec![addr1, addr2],
        vec![],
    );

    let result = coordinator.run(None).await.unwrap();

    let downloaded = std::fs::read(&output_path).unwrap();
    assert_eq!(downloaded.len(), data.len());
    assert_eq!(downloaded, data);
    assert!(result.pieces_verified > 0);
    assert!(result.peers_used >= 1);

    h1.abort();
    h2.abort();
}
