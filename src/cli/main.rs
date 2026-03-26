use clap::Parser;

#[derive(Parser)]
#[command(name = "aegis", version, about = "AegisTorrent P2P file distribution")]
enum Cli {
    /// Download from a torrent file
    Download {
        /// Path to .torrent file
        path: String,
    },
    /// Seed a file
    Seed {
        /// Path to file to seed
        path: String,
    },
}

fn main() {
    let _cli = Cli::parse();
}
