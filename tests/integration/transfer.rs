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
