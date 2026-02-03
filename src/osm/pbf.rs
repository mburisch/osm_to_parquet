use byteorder::{ByteOrder, NetworkEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{Read, Result},
};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    osm::blobs::BlobData,
    osmpbf::{Blob, BlobHeader},
};
use prost::Message;

#[derive(Debug)]
pub struct PbfReader<R: Read> {
    reader: R,
}

impl<R: Read> PbfReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Reads the next blob. Returns Ok(None) on clean EOF.
    pub fn read_blob(&mut self) -> Result<Option<BlobData>> {
        // Read the 4-byte header size (network endian). If EOF right away, return None.
        let header_size = match self.reader.read_u32::<NetworkEndian>() {
            Ok(v) => v as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        // Header
        let mut header_buf = vec![0; header_size];
        self.reader.read_exact(&mut header_buf)?;
        let header = BlobHeader::decode(&header_buf[..])?;

        // Blob payload
        let data_size = header.datasize as usize;
        let mut blob_buf = vec![0; data_size];
        self.reader.read_exact(&mut blob_buf)?;
        let blob = Blob::decode(&blob_buf[..])?;

        Ok(Some(BlobData::new(
            header,
            blob,
            4 + header_size + data_size,
        )))
    }
}

impl PbfReader<std::io::BufReader<File>> {
    pub fn for_local_file(filename: &str) -> Result<Self> {
        let f = File::open(filename)?;
        let b = std::io::BufReader::with_capacity(8 * 1024 * 1024, f);
        Ok(Self::new(b))
    }
}

impl<R: Read> Iterator for PbfReader<R> {
    type Item = BlobData;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_blob() {
            Ok(Some(data)) => Some(data),
            Ok(None) => None,
            Err(_) => None,
        }
    }
}

pub struct AsyncPbfReader<R: AsyncRead + Unpin> {
    reader: R,
}

impl<R: AsyncRead + Unpin> AsyncPbfReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    async fn read_u32_network(&mut self) -> std::io::Result<u32> {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).await?;
        Ok(NetworkEndian::read_u32(&buf))
    }

    /// Reads the next blob asynchronously. Returns Ok(None) on clean EOF.
    pub async fn read_blob(&mut self) -> Result<Option<BlobData>> {
        // Header size
        let header_size = match self.read_u32_network().await {
            Ok(v) => v as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        // Header
        let mut header_buf = vec![0; header_size];
        self.reader.read_exact(&mut header_buf).await?;
        let header = BlobHeader::decode(&header_buf[..])?;

        // Blob payload
        let data_size = header.datasize as usize;
        let mut blob_buf = vec![0; data_size];
        self.reader.read_exact(&mut blob_buf).await?;
        let blob = Blob::decode(&blob_buf[..])?;

        Ok(Some(BlobData::new(
            header,
            blob,
            4 + header_size + data_size,
        )))
    }
}
