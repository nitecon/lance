#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Parser)]
#[command(name = "lnc")]
#[command(about = "LANCE CLI tools for triage and maintenance")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Scan {
        #[arg(help = "Path to segment file")]
        path: PathBuf,
    },
    Repair {
        #[arg(help = "Path to segment file")]
        path: PathBuf,
    },
    RebuildIndex {
        #[arg(help = "Path to segment file")]
        path: PathBuf,
    },
    InspectIndex {
        #[arg(help = "Path to index file (.idx or .tidx)")]
        path: PathBuf,
    },
    FindRecovery {
        #[arg(help = "Path to data directory")]
        path: PathBuf,
    },
    Dump {
        #[arg(help = "Path to segment file")]
        path: PathBuf,
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
    /// Clean storage by removing orphaned/deleted topic directories
    Clean {
        #[arg(help = "Path to data directory")]
        path: PathBuf,
        #[arg(long, help = "Show what would be deleted without actually deleting")]
        dry_run: bool,
    },
    /// List all topics with their status
    Topics {
        #[arg(help = "Path to data directory")]
        path: PathBuf,
    },
    /// Fetch data from a topic at a specific offset (single fetch)
    Fetch {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
        #[arg(short, long, help = "Topic ID to fetch from")]
        topic: u32,
        #[arg(short, long, default_value = "0", help = "Byte offset to start from")]
        offset: u64,
        #[arg(short, long, default_value = "65536", help = "Maximum bytes to fetch")]
        max_bytes: u32,
        #[arg(long, help = "Output data in hex format")]
        hex: bool,
    },
    /// Consume data continuously from a topic with rewind/replay support
    Consume {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
        #[arg(short, long, help = "Topic ID to consume from")]
        topic: u32,
        #[arg(long, help = "Start from the beginning (offset 0)")]
        from_beginning: bool,
        #[arg(long, help = "Start from the end (tail mode)")]
        from_end: bool,
        #[arg(short, long, help = "Start from a specific byte offset")]
        offset: Option<u64>,
        #[arg(short, long, help = "Maximum number of fetches before stopping")]
        max_records: Option<usize>,
        #[arg(long, help = "Output data in hex format")]
        hex: bool,
    },
    /// Set retention policy for a topic
    SetRetention {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
        #[arg(short, long, help = "Topic ID")]
        topic: u32,
        #[arg(long, help = "Maximum age in seconds (0 = no limit)")]
        max_age_secs: u64,
        #[arg(long, help = "Maximum size in bytes (0 = no limit)")]
        max_bytes: u64,
    },
    /// Create a topic with retention policy
    CreateTopic {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
        #[arg(short, long, help = "Topic name")]
        name: String,
        #[arg(long, help = "Maximum age in seconds (0 = no limit)")]
        max_age_secs: Option<u64>,
        #[arg(long, help = "Maximum size in bytes (0 = no limit)")]
        max_bytes: Option<u64>,
    },
    /// List all topics from a remote server
    ListTopics {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
    },
    /// Get details of a specific topic from a remote server
    GetTopic {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
        #[arg(short, long, help = "Topic ID")]
        topic: u32,
    },
    /// Delete a topic from a remote server
    DeleteTopic {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
        #[arg(short, long, help = "Topic ID to delete")]
        topic: u32,
        #[arg(long, help = "Skip confirmation prompt")]
        force: bool,
    },
    /// Show cluster status and health
    ClusterStatus {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
    },
    /// Ingest data to a topic from file or stdin
    Ingest {
        #[arg(short, long, default_value = "127.0.0.1:1992", help = "Server address")]
        server: String,
        #[arg(short, long, help = "Topic ID to ingest to")]
        topic: u32,
        #[arg(short, long, help = "File to read data from (use '-' for stdin)")]
        file: Option<String>,
        #[arg(long, help = "Read input as newline-delimited records")]
        lines: bool,
        #[arg(long, default_value = "100", help = "Batch size for line mode")]
        batch_size: usize,
    },
    /// Verify data integrity for a topic (CRC checks)
    VerifyData {
        #[arg(short, long, help = "Path to data directory")]
        path: PathBuf,
        #[arg(short, long, help = "Topic ID to verify")]
        topic: u32,
        #[arg(long, help = "Stop on first error")]
        fail_fast: bool,
    },
    /// Export topic data to a file for backup
    ExportTopic {
        #[arg(short, long, help = "Path to data directory")]
        path: PathBuf,
        #[arg(short, long, help = "Topic ID to export")]
        topic: u32,
        #[arg(short, long, help = "Output file path")]
        output: PathBuf,
        #[arg(long, help = "Include metadata in export")]
        with_metadata: bool,
    },
    /// Import topic data from a backup file
    ImportTopic {
        #[arg(short, long, help = "Path to data directory")]
        path: PathBuf,
        #[arg(short, long, help = "Name for the imported topic")]
        name: String,
        #[arg(short, long, help = "Input file path")]
        input: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    init_tracing();

    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Scan { path } => lnc::commands::scan_segment(&path),
        Commands::Repair { path } => lnc::commands::repair_segment(&path),
        Commands::RebuildIndex { path } => lnc::commands::rebuild_index(&path),
        Commands::InspectIndex { path } => lnc::commands::inspect_index(&path),
        Commands::FindRecovery { path } => lnc::commands::find_recovery_candidates(&path),
        Commands::Dump { path, limit } => lnc::commands::dump_segment_records(&path, limit),
        Commands::Clean { path, dry_run } => lnc::commands::clean_storage(&path, dry_run),
        Commands::Topics { path } => lnc::commands::list_topics(&path),
        Commands::Fetch {
            server,
            topic,
            offset,
            max_bytes,
            hex,
        } => lnc::commands::fetch_topic(&server, topic, offset, max_bytes, hex).await,
        Commands::Consume {
            server,
            topic,
            from_beginning,
            from_end,
            offset,
            max_records,
            hex,
        } => {
            lnc::commands::consume_topic(
                &server,
                topic,
                offset,
                from_beginning,
                from_end,
                max_records,
                hex,
            )
            .await
        },
        Commands::SetRetention {
            server,
            topic,
            max_age_secs,
            max_bytes,
        } => lnc::commands::set_retention(&server, topic, max_age_secs, max_bytes).await,
        Commands::CreateTopic {
            server,
            name,
            max_age_secs,
            max_bytes,
        } => lnc::commands::create_topic(&server, &name, max_age_secs, max_bytes).await,
        Commands::ListTopics { server } => lnc::commands::list_topics_remote(&server).await,
        Commands::GetTopic { server, topic } => {
            lnc::commands::get_topic_remote(&server, topic).await
        },
        Commands::DeleteTopic {
            server,
            topic,
            force,
        } => lnc::commands::delete_topic_remote(&server, topic, force).await,
        Commands::ClusterStatus { server } => lnc::commands::cluster_status(&server).await,
        Commands::Ingest {
            server,
            topic,
            file,
            lines,
            batch_size,
        } => lnc::commands::ingest_data(&server, topic, file.as_deref(), lines, batch_size).await,
        Commands::VerifyData {
            path,
            topic,
            fail_fast,
        } => lnc::commands::verify_topic_data(&path, topic, fail_fast),
        Commands::ExportTopic {
            path,
            topic,
            output,
            with_metadata,
        } => lnc::commands::export_topic(&path, topic, &output, with_metadata),
        Commands::ImportTopic { path, name, input } => {
            lnc::commands::import_topic(&path, &name, &input)
        },
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false))
        .with(filter)
        .init();
}
