pub mod osmpbf {
    include!(concat!(env!("OUT_DIR"), "/osmpbf.rs"));
}

pub mod io;
pub mod osm;
pub mod parquet;
pub mod processor;
pub mod progress;
