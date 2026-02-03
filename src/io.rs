use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, parse_url, path::Path};
use std::io::{self, Result};
use tokio_util::io::StreamReader;
use url::Url;
// bytes::Bytes needed only if we expose the concrete StreamReader type; currently hidden behind impl AsyncRead.
// Removing unused import to satisfy lint.
use futures::StreamExt; // for map
use tokio::io::AsyncRead; // for return type if we choose impl AsyncRead

pub trait FileWriter {
    fn write_nodes(&self, data: Bytes) -> Result<()>;
    fn write_ways(&self, data: Bytes) -> Result<()>;
    fn write_relations(&self, data: Bytes) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct FileNameGenerator {
    nodes: Arc<AtomicUsize>,
    ways: Arc<AtomicUsize>,
    relations: Arc<AtomicUsize>,
}

impl FileNameGenerator {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(AtomicUsize::new(0)),
            ways: Arc::new(AtomicUsize::new(0)),
            relations: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn next_nodes(&self) -> String {
        let nodes_index = self.nodes.fetch_add(1, Ordering::Relaxed) + 1;
        format!("nodes/nodes_{nodes_index:06}.parquet")
    }

    pub fn next_ways(&self) -> String {
        let ways_index = self.ways.fetch_add(1, Ordering::Relaxed) + 1;
        format!("ways/ways_{ways_index:06}.parquet")
    }

    pub fn next_relations(&self) -> String {
        let relations_index = self.relations.fetch_add(1, Ordering::Relaxed) + 1;
        format!("relations/relations_{relations_index:06}.parquet")
    }
}

#[derive(Debug, Clone, Default)]
pub struct LocalFileWriter {
    root_path: PathBuf,
    names: FileNameGenerator,
}

impl LocalFileWriter {
    pub fn new(root_path: PathBuf) -> Self {
        Self {
            root_path: root_path,
            names: FileNameGenerator::new(),
        }
    }
}

impl FileWriter for LocalFileWriter {
    fn write_nodes(&self, data: Bytes) -> Result<()> {
        let filename = self.root_path.join(self.names.next_nodes());
        fs::write(filename, data)?;
        Ok(())
    }

    fn write_ways(&self, data: Bytes) -> Result<()> {
        let filename = self.root_path.join(self.names.next_ways());
        fs::write(filename, data)?;
        Ok(())
    }

    fn write_relations(&self, data: Bytes) -> Result<()> {
        let filename = self.root_path.join(self.names.next_relations());
        fs::write(filename, data)?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        if self.root_path.exists() {
            fs::remove_dir_all(&self.root_path)?;
        }
        fs::create_dir_all(&self.root_path)?;
        fs::create_dir_all(self.root_path.join("nodes"))?;
        fs::create_dir_all(self.root_path.join("ways"))?;
        fs::create_dir_all(self.root_path.join("relations"))?;
        Ok(())
    }
}

#[async_trait]
pub trait AsyncFileWriter {
    async fn write_nodes(&self, data: Bytes) -> Result<()>;
    async fn write_ways(&self, data: Bytes) -> Result<()>;
    async fn write_relations(&self, data: Bytes) -> Result<()>;
    async fn clear(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct ObjectStoreWriter {
    store: Arc<Box<dyn ObjectStore>>,
    root_path: Path,
    names: FileNameGenerator,
}

impl ObjectStoreWriter {
    pub fn new(root_path: &str) -> Self {
        let url = Url::parse(root_path).unwrap();
        let (store, path) = parse_url(&url).unwrap();
        Self {
            store: Arc::new(store),
            root_path: path,
            names: FileNameGenerator::new(),
        }
    }
}

#[async_trait]
impl AsyncFileWriter for ObjectStoreWriter {
    async fn write_nodes(&self, data: Bytes) -> Result<()> {
        let filename = Path::parse(format!("{}/{}", self.root_path, self.names.next_nodes()))
            .map_err(|e| std::io::Error::new(io::ErrorKind::Other, e))?;
        let payload = PutPayload::from_bytes(data);
        self.store.put(&filename, payload).await?;
        //fs::write(filename, data)?;
        Ok(())
    }

    async fn write_ways(&self, data: Bytes) -> Result<()> {
        let filename = Path::parse(format!("{}/{}", self.root_path, self.names.next_ways()))
            .map_err(|e| std::io::Error::new(io::ErrorKind::Other, e))?;
        let payload = PutPayload::from_bytes(data);
        self.store.put(&filename, payload).await?;
        Ok(())
    }

    async fn write_relations(&self, data: Bytes) -> Result<()> {
        let filename = Path::parse(format!(
            "{}/{}",
            self.root_path,
            self.names.next_relations()
        ))
        .map_err(|e| std::io::Error::new(io::ErrorKind::Other, e))?;
        let payload = PutPayload::from_bytes(data);
        self.store.put(&filename, payload).await?;
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        let locations = self
            .store
            .list(Some(&self.root_path))
            .filter_map(|meta_result| async {
                match meta_result {
                    Ok(meta) => Some(Ok(meta.location)),
                    Err(_) => None,
                }
            })
            .boxed();
        let mut status = self.store.delete_stream(locations);
        while let Some(result) = status.next().await {
            match result {
                Ok(_) => {}
                Err(_) => {}
            }
        }
        Ok(())
    }
}

pub async fn open_stream(url: &str) -> Result<impl AsyncRead + Unpin> {
    let url = Url::parse(url).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let (store, path) = parse_url(&url).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let raw_stream = store
        .get(&path)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        .into_stream();

    let io_stream = raw_stream.map(|res| res.map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
    let reader = StreamReader::new(io_stream);
    let buffered = tokio::io::BufReader::with_capacity(8 * 1024 * 1024, reader);
    Ok(buffered)
}
