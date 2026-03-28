use std::path::PathBuf;

use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::mpsc;

pub struct WriteCommand {
    pub offset: u64,
    pub data: Vec<u8>,
}

pub struct DiskWriter {
    tx: mpsc::Sender<WriteCommand>,
    handle: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DiskWriter {
    pub async fn new(path: PathBuf, total_size: u64) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        file.set_len(total_size).await?;
        drop(file);

        let (tx, mut rx) = mpsc::channel::<WriteCommand>(64);

        let handle = tokio::spawn(async move {
            let mut file = OpenOptions::new().write(true).open(&path).await?;
            while let Some(cmd) = rx.recv().await {
                file.seek(SeekFrom::Start(cmd.offset)).await?;
                file.write_all(&cmd.data).await?;
            }
            file.flush().await?;
            Ok(())
        });

        Ok(Self { tx, handle })
    }

    pub async fn write_piece(
        &self,
        index: u32,
        piece_size: usize,
        data: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<WriteCommand>> {
        let offset = index as u64 * piece_size as u64;
        self.tx.send(WriteCommand { offset, data }).await
    }

    pub async fn finish(self) -> std::io::Result<()> {
        drop(self.tx);
        self.handle.await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn writes_pieces_at_correct_offsets() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output.bin");
        let writer = DiskWriter::new(path.clone(), 12).await.unwrap();

        writer.write_piece(0, 4, b"AAAA".to_vec()).await.unwrap();
        writer.write_piece(1, 4, b"BBBB".to_vec()).await.unwrap();
        writer.write_piece(2, 4, b"CCCC".to_vec()).await.unwrap();
        writer.finish().await.unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data, b"AAAABBBBCCCC");
    }

    #[tokio::test]
    async fn handles_out_of_order_writes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output.bin");
        let writer = DiskWriter::new(path.clone(), 12).await.unwrap();

        writer.write_piece(2, 4, b"CCCC".to_vec()).await.unwrap();
        writer.write_piece(0, 4, b"AAAA".to_vec()).await.unwrap();
        writer.write_piece(1, 4, b"BBBB".to_vec()).await.unwrap();
        writer.finish().await.unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data, b"AAAABBBBCCCC");
    }

    #[tokio::test]
    async fn preallocates_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output.bin");
        let _writer = DiskWriter::new(path.clone(), 1024).await.unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 1024);
    }
}
