use std::time;

use indicatif::{MultiProgress, ProgressBar, ProgressFinish, ProgressStyle};

use crate::osm::types::ElementCount;

pub trait Progress
where
    Self: Clone + Send,
{
    fn inc_read_bytes(&self, bytes: u64);
    fn inc_pbf_blobs(&self, bytes: u64);
    fn inc_elements(&self, nodes: u64, ways: u64, relations: u64);
    fn inc_files(&self, files: u64);
    fn inc_write_bytes(&self, bytes: u64);
}

#[derive(Debug, Clone)]
pub struct ConsoleProgress {
    pub bar: MultiProgress,
    pub read: ProgressBar,
    pub pbf: ProgressBar,
    pub nodes: ProgressBar,
    pub ways: ProgressBar,
    pub relations: ProgressBar,
    pub files: ProgressBar,
    pub write: ProgressBar,
}

impl ConsoleProgress {
    pub fn new() -> Self {
        let bar = MultiProgress::new();
        let read = bar.add(Self::create_read_bar());
        let pbf = bar.add(Self::create_pbf_bar());
        let nodes = bar.add(Self::create_elements_bar("Nodes"));
        let ways = bar.add(Self::create_elements_bar("Ways"));
        let relations = bar.add(Self::create_elements_bar("Relations"));
        let files = bar.add(Self::create_files_bar());
        let write = bar.add(Self::create_write_bar());

        Self {
            bar,
            read,
            pbf,
            nodes,
            ways,
            relations,
            files,
            write,
        }
    }

    fn create_read_bar() -> ProgressBar {
        let style = ProgressStyle::with_template(
            "{msg:10} {spinner:.green}  [{elapsed:6}] [{binary_total_bytes} / {binary_bytes_per_sec}]",
        )
        .unwrap();
        Self::create_bar("Read", style)
    }

    fn create_pbf_bar() -> ProgressBar {
        let style =
            ProgressStyle::with_template("{msg:10} {spinner:.green}  [{elapsed:6}] [{human_len}]")
                .unwrap();
        Self::create_bar("PBF", style)
    }

    fn create_elements_bar(name: &str) -> ProgressBar {
        let style =
            ProgressStyle::with_template("{msg:10} {spinner:.green}  [{elapsed:6}] [{human_len}]")
                .unwrap();
        Self::create_bar(name, style)
    }

    fn create_files_bar() -> ProgressBar {
        let style =
            ProgressStyle::with_template("{msg:10} {spinner:.green}  [{elapsed:6}] [{human_len}]")
                .unwrap();
        Self::create_bar("Files", style)
    }

    fn create_write_bar() -> ProgressBar {
        let style = ProgressStyle::with_template(
            "{msg:10} {spinner:.green}  [{elapsed:6}] [{binary_total_bytes} / {binary_bytes_per_sec}]",
        )
        .unwrap();
        Self::create_bar("Write", style)
    }

    fn create_bar(name: &str, style: ProgressStyle) -> ProgressBar {
        let bar = ProgressBar::new_spinner().with_finish(ProgressFinish::AndLeave);
        bar.set_style(style);
        bar.enable_steady_tick(time::Duration::from_millis(100));
        bar.set_message(name.to_string());
        bar
    }
}

impl Progress for ConsoleProgress {
    fn inc_read_bytes(&self, bytes: u64) {
        self.read.inc(bytes);
    }

    fn inc_pbf_blobs(&self, bytes: u64) {
        self.pbf.inc(bytes);
    }

    fn inc_elements(&self, nodes: u64, ways: u64, relations: u64) {
        self.nodes.inc(nodes);
        self.ways.inc(ways);
        self.relations.inc(relations);
    }

    fn inc_files(&self, files: u64) {
        self.files.inc(files);
    }

    fn inc_write_bytes(&self, bytes: u64) {
        self.write.inc(bytes);
    }
}
