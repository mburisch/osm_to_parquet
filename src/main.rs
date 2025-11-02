use crossbeam_channel::bounded;
use osm_to_parquet::osm::elements::decode_primitive_block;
use osm_to_parquet::osm::{blobs::read_osm_data, elements::OsmData, pbf::PbfReader};
use osm_to_parquet::parquet::records::ElementBatches;
use osm_to_parquet::parquet::schemas::{get_node_schema, get_relation_schema, get_way_schema};
use osm_to_parquet::parquet::writer::{
    OsmParquetStreamWriter, ParquetData, ParquetMemoryStreamWriter,
};
use osm_to_parquet::progress::Progress;
use rayon;
use rayon::prelude::*;
use readable;
use readable::num;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn main() {
    //let filename = "/data/osm/nevada-latest.osm.pbf";
    let filename = "/data/osm/us-latest.osm.pbf";
    println!("Processing {}", filename);
    let pbf = PbfReader::for_local_file(filename).unwrap();

    let progress = Progress::new();

    let root_path = Path::new("/tmp/osm");
    if root_path.exists() {
        fs::remove_dir_all(root_path).unwrap();
    }
    fs::create_dir_all(root_path).unwrap();
    fs::create_dir_all(root_path.join("nodes")).unwrap();
    fs::create_dir_all(root_path.join("ways")).unwrap();
    fs::create_dir_all(root_path.join("relations")).unwrap();

    let (elements_sender, elements_receiver) = bounded(100);
    let (data_sender, data_receiver) = bounded(10);

    let pbf_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap();

    let parquet_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap();

    let write_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    pbf_pool.scope(|s| {
        {
            let progress = progress.clone();
            s.spawn(move |_| {
                pbf.into_iter().par_bridge().for_each(|blob| {
                    progress.inc_pbf(1);
                    let data = read_osm_data(&blob).unwrap();

                    match data {
                        OsmData::Primitive(block) => {
                            let elements =
                                ElementBatches::from_elements(&decode_primitive_block(&block));
                            progress.inc_elements(elements.count());
                            elements_sender.send(Arc::new(elements)).unwrap();
                        }
                        _ => {}
                    }
                });
                progress.pbf.finish();
                progress.elements.finish();
            });
        }

        parquet_pool.scope(|s| {
            for _ in 0..parquet_pool.current_num_threads() {
                let progress = progress.clone();
                let elements_receiver = elements_receiver.clone();
                let data_sender = data_sender.clone();
                s.spawn(move |_| {
                    let mut writer = OsmParquetStreamWriter::new(
                        Box::new(ParquetMemoryStreamWriter::new(
                            get_node_schema(),
                            None,
                            None,
                        )),
                        Box::new(ParquetMemoryStreamWriter::new(get_way_schema(), None, None)),
                        Box::new(ParquetMemoryStreamWriter::new(
                            get_relation_schema(),
                            None,
                            None,
                        )),
                    );
                    for elements in elements_receiver.iter() {
                        writer.write(&elements).unwrap();

                        for data in writer.flush(false).unwrap() {
                            progress.inc_files(1);
                            data_sender.send(data).unwrap();
                        }
                    }
                    for data in writer.flush(true).unwrap() {
                        progress.inc_files(1);
                        data_sender.send(data).unwrap();
                    }
                    progress.files.finish();
                });
            }
            drop(data_sender);

            write_pool.scope(|s| {
                for _ in 0..write_pool.current_num_threads() {
                    let nodes_index = Arc::new(AtomicUsize::new(1));
                    let ways_index = Arc::new(AtomicUsize::new(1));
                    let relations_index = Arc::new(AtomicUsize::new(1));
                    let progress = progress.clone();
                    let data_receiver = data_receiver.clone();
                    s.spawn(move |_s| {
                        for data in data_receiver.iter() {
                            match data {
                                ParquetData::Node(data) => {
                                    let nodes_index = nodes_index.fetch_add(1, Ordering::Relaxed);
                                    let filename = root_path
                                        .join(format!("nodes/nodes_{nodes_index:06}.parquet"));
                                    progress.inc_bytes(data.len());
                                    fs::write(filename, data).unwrap();
                                }
                                ParquetData::Way(data) => {
                                    let ways_index = ways_index.fetch_add(1, Ordering::Relaxed);
                                    let filename = root_path
                                        .join(format!("ways/ways_{ways_index:06}.parquet"));
                                    progress.inc_bytes(data.len());
                                    fs::write(filename, data).unwrap();
                                }
                                ParquetData::Relation(data) => {
                                    let relations_index =
                                        relations_index.fetch_add(1, Ordering::Relaxed);
                                    let filename = root_path.join(format!(
                                        "relations/relations_{relations_index:06}.parquet"
                                    ));
                                    progress.inc_bytes(data.len());
                                    fs::write(filename, data).unwrap();
                                }
                            }
                        }
                        progress.bytes.finish();
                    });
                }
            })
        });
    });
}
