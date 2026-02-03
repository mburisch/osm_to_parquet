use std::io::Read;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use std::io::Result;
use tokio::io::AsyncRead;

use crate::io::AsyncFileWriter;
use crate::osm::blobs::{BlobData, read_osm_data};
use crate::osm::elements::{OsmData, decode_primitive_block};
use crate::osm::pbf::{AsyncPbfReader, PbfReader};
use crate::parquet::records::Elements;
use crate::parquet::schemas::{get_node_schema, get_relation_schema, get_way_schema};
use crate::parquet::writer::{OsmParquetStreamWriter, ParquetData, ParquetMemoryStreamWriter};
use crate::progress::Progress;

pub fn generate_blobs<R: Read>(
    reader: PbfReader<R>,
    pbf_sender: Sender<Arc<BlobData>>,
    progress: impl Progress,
) {
    for blob in reader.into_iter() {
        progress.inc_pbf_blobs(1);
        pbf_sender.send(Arc::new(blob)).unwrap();
    }
}

pub async fn generate_blobs_async<R: AsyncRead + Unpin>(
    reader: &mut AsyncPbfReader<R>,
    pbf_sender: Sender<Arc<BlobData>>,
    progress: impl Progress,
) {
    while let Some(blob) = reader.read_blob().await.unwrap() {
        progress.inc_read_bytes(blob.size as u64);
        progress.inc_pbf_blobs(1);
        //pbf_sender.send(Arc::new(blob)).unwrap();
    }
}

pub fn process_blobs(
    pbf_receiver: Receiver<Arc<BlobData>>,
    elements_sender: Sender<Elements>,
    progress: impl Progress,
) {
    for blob in pbf_receiver.iter() {
        let data = read_osm_data(&blob).unwrap();
        match data {
            OsmData::Primitive(block) => {
                let elements = Elements::from_elements(&decode_primitive_block(&block));
                let count = elements.count();
                progress.inc_elements(
                    count.nodes as u64,
                    count.ways as u64,
                    count.relations as u64,
                );
                elements_sender.send(elements).unwrap();
            }
            _ => {}
        }
    }
}

pub fn generate_parquet(
    elements_receiver: Receiver<Elements>,
    data_sender: Sender<ParquetData>,
    progress: impl Progress,
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
            progress.inc_files(1);
            data_sender.send(data).unwrap();
        }
    }
    for data in writer.flush(true).unwrap() {
        progress.inc_files(1);
        data_sender.send(data).unwrap();
    }
}

// pub fn write_files(
//     data_receiver: Receiver<ParquetData>,
//     writer: impl FileWriter,
//     progress: impl Progress,
// ) {
//     for data in data_receiver.iter() {
//         match data {
//             ParquetData::Node(data) => {
//                 progress.inc_write_bytes(data.len() as u64);
//                 writer.write_nodes(data);
//             }
//             ParquetData::Way(data) => {
//                 progress.inc_write_bytes(data.len() as u64);
//                 writer.write_ways(data);
//             }
//             ParquetData::Relation(data) => {
//                 progress.inc_write_bytes(data.len() as u64);
//                 writer.write_relations(data);
//             }
//         }
//     }
// }

pub async fn write_files(
    data_receiver: Receiver<ParquetData>,
    writer: impl AsyncFileWriter,
    progress: impl Progress,
) -> Result<()> {
    for data in data_receiver.iter() {
        match data {
            ParquetData::Node(data) => {
                progress.inc_write_bytes(data.len() as u64);
                writer.write_nodes(data).await?;
            }
            ParquetData::Way(data) => {
                progress.inc_write_bytes(data.len() as u64);
                writer.write_ways(data).await?;
            }
            ParquetData::Relation(data) => {
                progress.inc_write_bytes(data.len() as u64);
                writer.write_relations(data).await?;
            }
        }
    }
    Ok(())
}
