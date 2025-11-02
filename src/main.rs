use clap::Parser;
use crossbeam_channel::bounded;
use osm_to_parquet::io::LocalFileWriter;
use osm_to_parquet::osm::pbf::PbfReader;
use osm_to_parquet::processor::{generate_blobs, generate_parquet, process_blobs, write_files};
use osm_to_parquet::progress::Progress;
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

#[tokio::main]
async fn main() {
    //let args = Args::parse();

    let filename = "/data/osm/nevada-latest.osm.pbf";
    //let filename = "/data/osm/us-latest.osm.pbf";
    println!("Processing {}", filename);
    let pbf = PbfReader::for_local_file(filename).unwrap();

    let progress = Progress::new();

    let root_path = Path::new("/tmp/osm");
    let writer = LocalFileWriter::new(root_path.to_path_buf());
    writer.clear();

    let (pbf_sender, pbf_receiver) = bounded(100);
    let (elements_sender, elements_receiver) = bounded(100);
    let (data_sender, data_receiver) = bounded(10);

    let blob_threads = 8;
    let parquet_threads = 16;
    let writer_threads = 2;

    thread::scope(|s| {
        {
            let progress = progress.pbf.clone();
            s.spawn(move || generate_blobs(pbf, pbf_sender, progress));
        }

        for _ in 0..blob_threads {
            let pbf_receiver = pbf_receiver.clone();
            let elements_sender = elements_sender.clone();
            let progress = progress.elements.clone();
            s.spawn(move || process_blobs(pbf_receiver, elements_sender, progress));
        }
        drop(pbf_receiver);
        drop(elements_sender);

        for _ in 0..parquet_threads {
            let elements_receiver = elements_receiver.clone();
            let data_sender = data_sender.clone();
            let progress = progress.files.clone();
            s.spawn(move || generate_parquet(elements_receiver, data_sender, progress));
        }
        drop(elements_receiver);
        drop(data_sender);

        for _ in 0..writer_threads {
            let data_receiver = data_receiver.clone();
            let writer = writer.clone();
            let progress = progress.bytes.clone();
            s.spawn(move || write_files(data_receiver, writer, progress));
        }
        drop(data_receiver);
    });
}
