use clap::{Parser, Subcommand};
use crossbeam_channel::bounded;
use osm_to_parquet::io::AsyncFileWriter;
use osm_to_parquet::io::{ObjectStoreWriter, open_stream};
use osm_to_parquet::osm::pbf::AsyncPbfReader;
use osm_to_parquet::parquet::blobs::BlobParquetConfig;
use osm_to_parquet::processor::{
    generate_blob_parquet, generate_blobs_async, generate_parquet, process_blobs, write_files,
};
use osm_to_parquet::progress::{ConsoleProgress, Progress};
use std::thread;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Convert a PBF file to per-element (nodes/ways/relations) Parquet files.
    Elements(ElementsArgs),
    /// Convert a PBF file to Parquet files with blob_index, blob_type and blob_data columns.
    Blobs(BlobsArgs),
}

#[derive(clap::Args, Debug)]
struct ElementsArgs {
    /// PBF filename (e.g. file:///path/to/data.osm.pbf, s3://..., https://...)
    #[arg(long)]
    pbf_filename: String,

    /// Output directory URL (e.g. file:///path/to/output, s3://bucket/prefix)
    #[arg(long)]
    output_path: String,

    /// Number of threads for blob decoding
    #[arg(long)]
    blob_threads: Option<usize>,

    /// Number of threads for Parquet encoding
    #[arg(long)]
    parquet_threads: Option<usize>,

    /// Number of threads for writing files
    #[arg(long)]
    writer_threads: Option<usize>,
}

#[derive(clap::Args, Debug)]
struct BlobsArgs {
    /// PBF filename (e.g. file:///path/to/data.osm.pbf, s3://..., https://...)
    #[arg(long)]
    pbf_filename: String,

    /// Output directory URL (e.g. file:///path/to/output, s3://bucket/prefix)
    #[arg(long)]
    output_path: String,

    /// Maximum number of blobs per Parquet file.
    #[arg(long)]
    max_blobs_per_file: Option<usize>,

    /// Maximum total size of stored blob_data bytes per Parquet file.
    #[arg(long)]
    max_file_size_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
struct ThreadConfig {
    blob_threads: usize,
    parquet_threads: usize,
    writer_threads: usize,
}

impl ThreadConfig {
    pub fn default() -> Self {
        Self {
            blob_threads: 8,
            parquet_threads: 16,
            writer_threads: 2,
        }
    }
}

async fn process_pbf(
    pbf_filename: &str,
    output_path: &str,
    thread_config: ThreadConfig,
    progress: impl Progress + 'static,
) {
    let writer = ObjectStoreWriter::new(output_path);

    writer.clear().await.unwrap();

    let (pbf_sender, pbf_receiver) = bounded(100);
    let (elements_sender, elements_receiver) = bounded(100);
    let (data_sender, data_receiver) = bounded(10);

    thread::scope(|s| {
        {
            let progress = progress.clone();
            let pbf_filename = pbf_filename.to_string();
            tokio::spawn(async move {
                let mut pbf =
                    AsyncPbfReader::new(open_stream(pbf_filename.as_str()).await.unwrap());
                generate_blobs_async(&mut pbf, pbf_sender, progress).await;
            });
        }

        for _ in 0..thread_config.blob_threads {
            let pbf_receiver = pbf_receiver.clone();
            let elements_sender = elements_sender.clone();
            let progress = progress.clone();
            s.spawn(move || process_blobs(pbf_receiver, elements_sender, progress));
        }
        drop(pbf_receiver);
        drop(elements_sender);

        for _ in 0..thread_config.parquet_threads {
            let elements_receiver = elements_receiver.clone();
            let data_sender = data_sender.clone();
            let progress = progress.clone();
            s.spawn(move || generate_parquet(elements_receiver, data_sender, progress));
        }
        drop(elements_receiver);
        drop(data_sender);

        for _ in 0..thread_config.writer_threads {
            let data_receiver = data_receiver.clone();
            let writer = writer.clone();
            let progress = progress.clone();
            tokio::spawn(async move {
                write_files(data_receiver, writer, progress).await.unwrap();
            });
        }
        drop(data_receiver);
    });
}

async fn process_pbf_blobs(
    pbf_filename: &str,
    output_path: &str,
    config: BlobParquetConfig,
    progress: impl Progress + 'static,
) {
    let writer = ObjectStoreWriter::new(output_path);
    writer.clear().await.unwrap();

    let mut pbf = AsyncPbfReader::new(open_stream(pbf_filename).await.unwrap());
    generate_blob_parquet(&mut pbf, writer, config, progress)
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let progress = ConsoleProgress::new();

    match args.command {
        Command::Elements(args) => {
            let mut thread_config = ThreadConfig::default();
            if let Some(n) = args.blob_threads {
                thread_config.blob_threads = n;
            }
            if let Some(n) = args.parquet_threads {
                thread_config.parquet_threads = n;
            }
            if let Some(n) = args.writer_threads {
                thread_config.writer_threads = n;
            }
            process_pbf(&args.pbf_filename, &args.output_path, thread_config, progress).await;
        }
        Command::Blobs(args) => {
            let config = if args.max_blobs_per_file.is_none() && args.max_file_size_bytes.is_none()
            {
                BlobParquetConfig::default()
            } else {
                BlobParquetConfig {
                    max_blobs_per_file: args.max_blobs_per_file,
                    max_file_size_bytes: args.max_file_size_bytes,
                }
            };
            process_pbf_blobs(&args.pbf_filename, &args.output_path, config, progress).await;
        }
    }
}
