use std::{collections::HashMap, fmt, sync::Arc};

pub type OsmTags = HashMap<Arc<String>, Arc<String>>;

#[derive(Debug, Clone, Default)]
pub struct OsmInfo {
    pub version: i32,
    pub timestamp: i64,
    pub changeset: i64,
    pub uid: i64,
    pub user_sid: Arc<String>,
}

#[derive(Debug, Default)]
pub struct OsmNode {
    pub id: i64,
    pub info: OsmInfo,
    pub tags: OsmTags,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Default)]
pub struct OsmWay {
    pub id: i64,
    pub info: OsmInfo,
    pub tags: OsmTags,
    pub nodes: Vec<i64>,
}

#[derive(Debug)]
pub struct OsmRelationMember {
    pub role: Arc<String>,
    pub id: i64,
    pub member_type: Arc<String>,
}

#[derive(Debug)]
pub struct OsmRelation {
    pub id: i64,
    pub info: OsmInfo,
    pub tags: OsmTags,
    pub members: Vec<OsmRelationMember>,
}

#[derive(Debug, Default, Clone)]
pub struct OsmElements {
    pub nodes: Vec<Arc<OsmNode>>,
    pub ways: Vec<Arc<OsmWay>>,
    pub relations: Vec<Arc<OsmRelation>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ElementCount {
    pub nodes: usize,
    pub ways: usize,
    pub relations: usize,
}

impl ElementCount {
    pub fn new(nodes: usize, ways: usize, relations: usize) -> Self {
        Self {
            nodes,
            ways,
            relations,
        }
    }

    pub fn total(&self) -> usize {
        self.nodes + self.ways + self.relations
    }
}

impl fmt::Display for ElementCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(nodes = {} / ways = {} / relations = {})",
            readable::num::Unsigned::from(self.nodes),
            readable::num::Unsigned::from(self.ways),
            readable::num::Unsigned::from(self.relations)
        )
    }
}
