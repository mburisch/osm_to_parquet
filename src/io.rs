use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub trait FileWriter {
    fn write_nodes(&self, data: &[u8]);
    fn write_ways(&self, data: &[u8]);
    fn write_relations(&self, data: &[u8]);
}

#[derive(Debug, Clone, Default)]
pub struct LocalFileWriter {
    root_path: PathBuf,
    nodes: Arc<AtomicUsize>,
    ways: Arc<AtomicUsize>,
    relations: Arc<AtomicUsize>,
}

impl LocalFileWriter {
    pub fn new(root_path: PathBuf) -> Self {
        Self {
            root_path: root_path,
            nodes: Arc::new(AtomicUsize::new(0)),
            ways: Arc::new(AtomicUsize::new(0)),
            relations: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn clear(&self) {
        if self.root_path.exists() {
            fs::remove_dir_all(&self.root_path).unwrap();
        }
        fs::create_dir_all(&self.root_path).unwrap();
        fs::create_dir_all(self.root_path.join("nodes")).unwrap();
        fs::create_dir_all(self.root_path.join("ways")).unwrap();
        fs::create_dir_all(self.root_path.join("relations")).unwrap();
    }
}

impl FileWriter for LocalFileWriter {
    fn write_nodes(&self, data: &[u8]) {
        let nodes_index = self.nodes.fetch_add(1, Ordering::Relaxed) + 1;
        let filename = self
            .root_path
            .join(format!("nodes/nodes_{nodes_index:06}.parquet"));
        fs::write(filename, data).unwrap();
    }

    fn write_ways(&self, data: &[u8]) {
        let ways_index = self.ways.fetch_add(1, Ordering::Relaxed) + 1;
        let filename = self
            .root_path
            .join(format!("ways/ways_{ways_index:06}.parquet"));
        fs::write(filename, data).unwrap();
    }

    fn write_relations(&self, data: &[u8]) {
        let relations_index = self.relations.fetch_add(1, Ordering::Relaxed) + 1;
        let filename = self
            .root_path
            .join(format!("relations/relations_{relations_index:06}.parquet"));
        fs::write(filename, data).unwrap();
    }
}

// pub struct ObjectStoreReader<Source> {
//     source: Source,
//     store: ObjectStore,
// }

// impl ObjectStoreReader {
//     pub fn new(filename: &str) -> Self {
//         let url = Url::parse(filename).unwrap();
//         let (store, path) = parse_url(&url).unwrap();

//         // let stream = store.get(&path).await.unwrap().into_stream();
//         //stream.try_into()

//         Self {
//             store: store,
//             path: path.to_string(),
//         }
//     }
// }

// impl Read for ObjectStoreReader {
//     fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
//         Ok(0)
//     }
// }
