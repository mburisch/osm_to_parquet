use clap::Parser;
use crossbeam_channel::bounded;
use osm_to_parquet::io::AsyncFileWriter;
use osm_to_parquet::io::{ObjectStoreWriter, open_stream};
use osm_to_parquet::osm::pbf::{AsyncPbfReader, PbfReader};
use osm_to_parquet::processor::{
    generate_blobs_async, generate_parquet, process_blobs, write_files,
};
use osm_to_parquet::progress::{ConsoleProgress, Progress};
use std::path::Path;
use std::thread;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// PBF filename
    #[arg(long)]
    pbf_filename: String,

    /// Output directory
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

        // for _ in 0..thread_config.writer_threads {
        //     let data_receiver = data_receiver.clone();
        //     let writer = writer.clone();
        //     let progress = progress.clone();
        //     s.spawn(move || write_files(data_receiver, writer, progress).unwrap());
        // }
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

async fn async_file_reader(progress: impl Progress + 'static) {
    let filename = "file:///data/osm/us-latest.osm.pbf";
    let mut pbf = AsyncPbfReader::new(open_stream(filename).await.unwrap());
    while let Some(blob) = pbf.read_blob().await.unwrap() {
        progress.inc_read_bytes(blob.size as u64);
        progress.inc_pbf_blobs(1);
        //pbf_sender.send(Arc::new(blob)).unwrap();
    }
}

async fn file_reader(progress: impl Progress + 'static) {
    let filename = "/Users/mburisch/src/data/osm/us-latest.osm.pbf";
    let mut pbf = PbfReader::for_local_file(filename).unwrap();

    while let Some(blob) = pbf.read_blob().unwrap() {
        progress.inc_read_bytes(blob.size as u64);
        progress.inc_pbf_blobs(1);
        //pbf_sender.send(Arc::new(blob)).unwrap();
    }
}

#[tokio::main]
async fn main() {
    //let args = Args::parse();

    //let filename = "/data/osm/nevada-latest.osm.pbf";
    //let filename = "/data/osm/us-latest.osm.pbf";
    let url = "file:///Users/mburisch/src/data/osm/us-latest.osm.pbf";
    //let url = "file:///data/osm/nevada-latest.osm.pbf";
    //let url = "https://download.geofabrik.de/europe/isle-of-man-latest.osm.pbf";
    //let url = "https://download.geofabrik.de/europe/greece-latest.osm.pbf";

    let output_path = "file:///Users/mburisch/src/data/parquet";

    let progress = ConsoleProgress::new();
    let thread_config = ThreadConfig::default();

    //file_reader(progress.clone()).await;

    process_pbf(url, output_path, thread_config, progress).await;
}
