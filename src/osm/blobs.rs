use std::io::{Cursor, ErrorKind, Read, Result};

use flate2::bufread::ZlibDecoder;
use lzma_rs::lzma2_decompress;

use crate::{
    osm::elements::OsmData,
    osmpbf::{Blob, BlobHeader, HeaderBlock, PrimitiveBlock, blob::Data},
};
use prost::Message;

#[derive(Debug)]
pub struct BlobData {
    pub header: BlobHeader,
    pub blob: Blob,
}

impl BlobData {
    pub fn new(header: BlobHeader, blob: Blob) -> Self {
        Self { header, blob }
    }
}

fn get_blob_data(blob: &Blob) -> Result<Vec<u8>> {
    match &blob.data {
        Some(Data::Raw(data)) => Ok(data.clone()),
        Some(Data::ZlibData(data)) => {
            let mut decoder = ZlibDecoder::new(&data[..]);
            let mut buffer = vec![0; blob.raw_size.ok_or(ErrorKind::InvalidInput)? as usize];
            decoder.read(&mut buffer)?;
            return Ok(buffer);
        }
        Some(Data::Lz4Data(data)) => {
            let mut decoder = lz4::Decoder::new(&data[..])?;
            let mut buffer = vec![0; blob.raw_size.ok_or(ErrorKind::InvalidInput)? as usize];
            decoder.read(&mut buffer)?;
            return Ok(buffer);
        }
        Some(Data::LzmaData(data)) => {
            let mut buffer = vec![0; blob.raw_size.ok_or(ErrorKind::InvalidInput)? as usize];
            let mut cursor = Cursor::new(&data[..]);
            lzma2_decompress(&mut cursor, &mut buffer).map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidInput, "LZMA decompression failed")
            })?;
            return Ok(buffer);
        }
        Some(Data::ZstdData(data)) => {
            let mut decoder = zstd::Decoder::new(&data[..])?;
            let mut buffer = vec![0; blob.raw_size.ok_or(ErrorKind::InvalidInput)? as usize];
            decoder.read(&mut buffer)?;
            return Ok(buffer);
        }
        _ => Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "Unsupported compression",
        )),
    }
}

pub fn read_osm_data(data: &BlobData) -> Result<OsmData> {
    let buffer = get_blob_data(&data.blob)?;

    match data.header.r#type.as_str() {
        "OSMHeader" => {
            let header_block = HeaderBlock::decode(&buffer[..])?;
            Ok(OsmData::Header(header_block))
        }
        "OSMData" => {
            let primitive_block = PrimitiveBlock::decode(&buffer[..])?;
            Ok(OsmData::Primitive(primitive_block))
        }
        _ => Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "Invalid packet type",
        )),
    }
}
