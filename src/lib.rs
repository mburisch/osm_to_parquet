pub mod osmpbf {
    include!(concat!(env!("OUT_DIR"), "/osmpbf.rs"));
}

pub mod osm;
pub mod parquet;
pub mod progress;
