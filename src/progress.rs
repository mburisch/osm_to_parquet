use std::time;

use indicatif::{MultiProgress, ProgressBar, ProgressFinish, ProgressStyle};

use crate::osm::types::ElementCount;

#[derive(Debug, Clone)]
pub struct ElementsProgress {
    pub nodes: ProgressBar,
    pub ways: ProgressBar,
    pub relations: ProgressBar,
}

impl ElementsProgress {
    pub fn new(nodes: ProgressBar, ways: ProgressBar, relations: ProgressBar) -> Self {
        Self {
            nodes,
            ways,
            relations,
        }
    }

    pub fn inc(&self, count: ElementCount) {
        self.nodes.inc(count.nodes as u64);
        self.ways.inc(count.ways as u64);
        self.relations.inc(count.relations as u64);
    }

    pub fn finish(&self) {
        self.nodes.finish();
        self.ways.finish();
        self.relations.finish();
    }
}

#[derive(Debug, Clone)]
pub struct Progress {
    pub bar: MultiProgress,
    pub pbf: ProgressBar,
    pub elements: ElementsProgress,
    pub files: ProgressBar,
    pub bytes: ProgressBar,
}

impl Progress {
    pub fn new() -> Self {
        let bar = MultiProgress::new();
        let pbf = bar.add(Self::create_pbf_bar());
        let el = Self::create_elements_bar();
        let elements =
            ElementsProgress::new(bar.add(el.nodes), bar.add(el.ways), bar.add(el.relations));
        let files = bar.add(Self::create_files_bar());
        let bytes = bar.add(Self::create_bytes_bar());

        Self {
            bar,
            pbf,
            elements,
            files,
            bytes,
        }
    }

    fn create_pbf_bar() -> ProgressBar {
        let style = ProgressStyle::with_template(
            "{msg:10} {spinner:.green}  [{elapsed:6}] [{human_len} {per_sec}]",
        )
        .unwrap();
        Self::create_bar("PBF", style)
    }

    fn create_elements_bar() -> ElementsProgress {
        let style =
            ProgressStyle::with_template("{msg:10} {spinner:.green}  [{elapsed:6}] [{human_len}]")
                .unwrap();

        ElementsProgress::new(
            Self::create_bar("Nodes", style.clone()),
            Self::create_bar("Ways", style.clone()),
            Self::create_bar("Relations", style.clone()),
        )
    }

    fn create_files_bar() -> ProgressBar {
        let style =
            ProgressStyle::with_template("{msg:10} {spinner:.green}  [{elapsed:6}] [{human_len}]")
                .unwrap();
        Self::create_bar("Files", style)
    }

    fn create_bytes_bar() -> ProgressBar {
        let style = ProgressStyle::with_template(
            "{msg:10} {spinner:.green}  [{elapsed:6}] [{binary_total_bytes} / {binary_bytes_per_sec}]",
        )
        .unwrap();
        Self::create_bar("Bytes", style)
    }

    fn create_bar(name: &str, style: ProgressStyle) -> ProgressBar {
        let bar = ProgressBar::new_spinner().with_finish(ProgressFinish::AndLeave);
        bar.set_style(style);
        bar.enable_steady_tick(time::Duration::from_millis(100));
        bar.set_message(name.to_string());
        bar
    }
}
