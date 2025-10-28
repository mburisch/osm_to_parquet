use indicatif::{ProgressBar, ProgressStyle};
use osm_to_parquet::{
    osm::{blobs::read_osm_data, elements::OsmData, pbf::PbfReader},
    parquet::parquet::{OsmParquetWriter, WriteStatistics, WriterConfig},
};
use rayon::prelude::*;
use std::time;

fn make_progress_bar() -> ProgressBar {
    let bar = ProgressBar::new_spinner();
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{human_pos} / {per_sec}] {msg} [{elapsed}]",
        )
        .unwrap(),
    );
    bar.enable_steady_tick(time::Duration::from_millis(100));
    bar
}

fn main() {
    let filename = "/workspaces/data/osm/nevada-latest.osm.pbf";
    //let filename = "/workspaces/data/osm/us-latest.osm.pbf";
    println!("Processing {}", filename);
    let pbf = PbfReader::with_filename(filename).unwrap();

    let progress_bar = make_progress_bar();

    let config = WriterConfig::new();

    let root_path = "/tmp/osm";

    // rayon::ThreadPoolBuilder::new()
    //     .num_threads(2)
    //     .build_global()
    //     .unwrap();

    let r: usize = pbf
        .par_bridge()
        .map_init(
            || OsmParquetWriter::new(root_path, config.clone(), config.clone(), config.clone()),
            |writer, blob| {
                let data = read_osm_data(&blob).unwrap();
                if let OsmData::Primitive(block) = data {
                    writer.write_elements(&block)
                } else {
                    WriteStatistics::default()
                }
            },
        )
        .map(|s| progress_bar.inc(s.total() as u64))
        .count();
    let result = r;
    println!("Finished processing: {:?}", result);
}
