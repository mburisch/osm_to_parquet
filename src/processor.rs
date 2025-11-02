use std::io::Read;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use indicatif::ProgressBar;

use crate::io::FileWriter;
use crate::osm::blobs::{BlobData, read_osm_data};
use crate::osm::elements::{OsmData, decode_primitive_block};
use crate::osm::pbf::PbfReader;
use crate::parquet::records::Elements;
use crate::parquet::schemas::{get_node_schema, get_relation_schema, get_way_schema};
use crate::parquet::writer::{OsmParquetStreamWriter, ParquetData, ParquetMemoryStreamWriter};
use crate::progress::ElementsProgress;

pub fn generate_blobs<R: Read>(
    reader: PbfReader<R>,
    pbf_sender: Sender<Arc<BlobData>>,
    progress: ProgressBar,
) {
    for blob in reader.into_iter() {
        progress.inc(1);
        pbf_sender.send(Arc::new(blob)).unwrap();
    }
    progress.finish();
}

pub fn process_blobs(
    pbf_receiver: Receiver<Arc<BlobData>>,
    elements_sender: Sender<Elements>,
    progress: ElementsProgress,
) {
    for blob in pbf_receiver.iter() {
        let data = read_osm_data(&blob).unwrap();
        match data {
            OsmData::Primitive(block) => {
                let elements = Elements::from_elements(&decode_primitive_block(&block));
                progress.inc(elements.count());
                elements_sender.send(elements).unwrap();
            }
            _ => {}
        }
    }
    progress.finish();
}

pub fn generate_parquet(
    elements_receiver: Receiver<Elements>,
    data_sender: Sender<ParquetData>,
    progress: ProgressBar,
) {
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
            progress.inc(1);
            data_sender.send(data).unwrap();
        }
    }
    for data in writer.flush(true).unwrap() {
        progress.inc(1);
        data_sender.send(data).unwrap();
    }
    progress.finish();
}

pub fn write_files(
    data_receiver: Receiver<ParquetData>,
    writer: impl FileWriter,
    progress: ProgressBar,
) {
    for data in data_receiver.iter() {
        match data {
            ParquetData::Node(data) => {
                progress.inc(data.len() as u64);
                writer.write_nodes(&data);
            }
            ParquetData::Way(data) => {
                progress.inc(data.len() as u64);
                writer.write_ways(&data);
            }
            ParquetData::Relation(data) => {
                progress.inc(data.len() as u64);
                writer.write_relations(&data);
            }
        }
    }
    progress.finish();
}
