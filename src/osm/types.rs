use std::{collections::HashMap, sync::Arc};

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

#[derive(Debug)]
pub enum OsmElement {
    Node(Arc<OsmNode>),
    Way(Arc<OsmWay>),
    Relation(Arc<OsmRelation>),
}
