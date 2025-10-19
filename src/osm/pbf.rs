use byteorder::{NetworkEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read, Result},
};

use crate::{
    osm::blobs::BlobData,
    osmpbf::{Blob, BlobHeader},
};
use prost::Message;

#[derive(Debug)]
pub struct PbfReader<Source: Read> {
    source: Source,
}

impl<Source: Read> PbfReader<Source> {
    pub fn new(source: Source) -> Self {
        Self { source: source }
    }

    pub fn read_blob(&mut self) -> Result<BlobData> {
        // Header
        let header_size = self.source.read_u32::<NetworkEndian>()? as usize;
        let mut header_buf = vec![0; header_size];
        self.source.read_exact(&mut header_buf[0..header_size])?;
        let header = BlobHeader::decode(&header_buf[..header_size])?;

        // Blob
        let data_size = header.datasize as usize;
        let mut blob_buf = vec![0; data_size];
        self.source.read_exact(&mut blob_buf[0..data_size])?;
        let blob = Blob::decode(&blob_buf[..data_size])?;

        Ok(BlobData::new(header, blob))
    }
}

impl PbfReader<BufReader<File>> {
    pub fn with_filename(filename: &str) -> Result<Self> {
        let f = File::open(filename)?;
        let b = BufReader::with_capacity(8 * 1024 * 1024, f);
        Ok(Self::new(b))
    }
}

impl<Source: Read> Iterator for PbfReader<Source> {
    type Item = BlobData;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_blob() {
            Ok(data) => Some(data),
            Err(_) => None,
        }
    }
}
