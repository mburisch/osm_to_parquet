use arrow::{array::RecordBatch, datatypes::Schema};
use crossbeam_channel::{Receiver, Sender, bounded};
use indicatif::{ProgressBar, ProgressStyle};
use osm_to_parquet::{
    osm::{
        blobs::{BlobData, read_osm_data},
        elements::{OsmData, decode_primitive_block},
        pbf::PbfReader,
        types::{OsmElement, OsmNode, OsmRelation, OsmWay},
    },
    parquet::{
        parquet::{ParquetFilenameGenerator, ParquetWriter},
        records::RecordBatchConverter,
        schemas::{get_node_schema, get_relation_schema, get_way_schema},
    },
    pipeline::{OsmChannels, OsmSendChannels},
};
use std::{io::Read, sync::Arc, thread, time};

fn blob_reader_func<Source: Read>(
    pbf: PbfReader<Source>,
    blob_sender: Sender<BlobData>,
) -> impl FnOnce() {
    move || {
        for blob in pbf.into_iter() {
            blob_sender.send(blob).unwrap();
        }
    }
}

fn element_decoder_func(
    blob_receiver: Receiver<BlobData>,
    osm_channels: OsmSendChannels<OsmNode, OsmWay, OsmRelation>,
) -> impl Fn() {
    move || {
        for blob in blob_receiver.iter() {
            let data = read_osm_data(&blob).unwrap();
            let elements = match data {
                OsmData::Header(_) => Vec::new(),
                OsmData::Primitive(primitive) => decode_primitive_block(&primitive),
            };
            for element in elements {
                match element {
                    OsmElement::Node(node) => {
                        osm_channels.node.send(node).unwrap();
                    }
                    OsmElement::Way(way) => {
                        osm_channels.way.send(way).unwrap();
                    }
                    OsmElement::Relation(relation) => {
                        osm_channels.relation.send(relation).unwrap();
                    }
                }
            }
        }
    }
}

fn record_batch_generator<T>(
    receiver: Receiver<Arc<T>>,
    sender: Sender<Arc<RecordBatch>>,
    capacity: usize,
) -> impl Fn()
where
    T: RecordBatchConverter<T>,
{
    move || {
        let mut batch = Vec::with_capacity(capacity);
        for item in receiver.iter() {
            batch.push(item);
            if batch.len() == capacity {
                let record_batch = T::create_record_batch(&batch);
                sender.send(record_batch).unwrap();
                batch.clear();
            }
        }
    }
}

fn parquet_writer_func(
    receiver: Receiver<Arc<RecordBatch>>,
    filename_getter: ParquetFilenameGenerator,
    schema: Arc<Schema>,
    bar: ProgressBar,
) -> impl FnOnce() {
    move || {
        let mut writer = ParquetWriter::new(schema.clone(), filename_getter, 128_000_000);

        for item in receiver.iter() {
            writer.write(&item);
            bar.inc(item.num_rows() as u64);
        }
        writer.close();
    }
}

fn make_pbar() -> ProgressBar {
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
    //let filename = "/workspaces/data/osm/nevada-latest.osm.pbf";
    let filename = "/workspaces/data/osm/us-latest.osm.pbf";
    println!("Processing {}", filename);
    let pbf = PbfReader::with_filename(filename).unwrap();

    let bar = make_pbar();

    let (blob_sender, blob_receiver) = bounded(100);
    let osm_channels = OsmChannels::new(100_000);
    let record_channels = OsmChannels::new(100_000);

    thread::scope(|s| {
        s.spawn(blob_reader_func(pbf, blob_sender));

        for _ in 0..6 {
            s.spawn(element_decoder_func(
                blob_receiver.clone(),
                osm_channels.send_channels(),
            ));
        }

        for _ in 0..8 {
            s.spawn(record_batch_generator(
                osm_channels.node_receiver.clone(),
                record_channels.node_sender.clone(),
                1000,
            ));
            s.spawn(record_batch_generator(
                osm_channels.way_receiver.clone(),
                record_channels.way_sender.clone(),
                1000,
            ));
            s.spawn(record_batch_generator(
                osm_channels.relation_receiver.clone(),
                record_channels.relation_sender.clone(),
                1000,
            ));
        }

        for i in 0..6 {
            s.spawn(parquet_writer_func(
                record_channels.node_receiver.clone(),
                ParquetFilenameGenerator::new("/tmp/osm", "nodes", i + 1),
                get_node_schema(),
                bar.clone(),
            ));
            s.spawn(parquet_writer_func(
                record_channels.way_receiver.clone(),
                ParquetFilenameGenerator::new("/tmp/osm", "ways", i + 1),
                get_way_schema(),
                bar.clone(),
            ));
            s.spawn(parquet_writer_func(
                record_channels.relation_receiver.clone(),
                ParquetFilenameGenerator::new("/tmp/osm", "relations", i + 1),
                get_relation_schema(),
                bar.clone(),
            ));
        }
        drop(osm_channels);
        drop(record_channels);
    });

    let result = 0;
    println!("Finished processing: {:?}", result);
}
