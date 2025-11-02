use std::{
    collections::HashMap,
    iter::zip,
    ops,
    sync::{Arc, OnceLock},
};

use crate::{
    osm::types::{OsmElements, OsmInfo, OsmNode, OsmRelation, OsmRelationMember, OsmTags, OsmWay},
    osmpbf::{self, HeaderBlock, PrimitiveBlock},
};

use itertools::izip;

pub fn get_node_string() -> Arc<String> {
    static SCHEMA: OnceLock<Arc<String>> = OnceLock::new();
    let s = SCHEMA.get_or_init(|| Arc::new("node".to_string()));
    s.clone()
}

pub fn get_way_string() -> Arc<String> {
    static SCHEMA: OnceLock<Arc<String>> = OnceLock::new();
    let s = SCHEMA.get_or_init(|| Arc::new("way".to_string()));
    s.clone()
}

pub fn get_relation_string() -> Arc<String> {
    static SCHEMA: OnceLock<Arc<String>> = OnceLock::new();
    let s = SCHEMA.get_or_init(|| Arc::new("relation".to_string()));
    s.clone()
}

pub enum OsmData {
    Header(HeaderBlock),
    Primitive(PrimitiveBlock),
}

struct ValueDecoder {
    granularity: i64,
    lat_offset: i64,
    lon_offset: i64,
    date_granularity: i64,
}

impl ValueDecoder {
    fn new(block: &osmpbf::PrimitiveBlock) -> Self {
        Self {
            granularity: block.granularity.unwrap_or(100) as i64,
            lat_offset: block.lat_offset.unwrap_or(0) as i64,
            lon_offset: block.lon_offset.unwrap_or(0) as i64,
            date_granularity: block.date_granularity.unwrap_or(1000) as i64,
        }
    }

    fn latitude(&self, value: i64) -> f64 {
        0.000000001 * (self.lat_offset + (self.granularity * value)) as f64
    }

    fn longitude(&self, value: i64) -> f64 {
        0.000000001 * (self.lon_offset + (self.granularity * value)) as f64
    }

    fn timestamp(&self, value: i64) -> i64 {
        value * self.date_granularity
    }
}

pub trait DeltaDecoderIterator<T>: Iterator<Item = T> + Sized {
    fn delta_decode(self) -> impl Iterator<Item = T>;
}

impl<T, I: Iterator<Item = T>> DeltaDecoderIterator<T> for I
where
    T: num_traits::PrimInt + ops::AddAssign,
{
    fn delta_decode(self) -> impl Iterator<Item = T> {
        let mut current = T::zero();
        self.map(move |x| {
            current += x;
            current
        })
    }
}

pub struct PrimitiveBlockDecoder {
    strings: Vec<Arc<String>>,
    decoder: ValueDecoder,
}

impl PrimitiveBlockDecoder {
    pub fn new(block: &osmpbf::PrimitiveBlock) -> PrimitiveBlockDecoder {
        PrimitiveBlockDecoder {
            strings: Self::string_table(block),
            decoder: ValueDecoder::new(&block),
        }
    }

    fn string_table(block: &osmpbf::PrimitiveBlock) -> Vec<Arc<String>> {
        let strings: Vec<_> = block
            .stringtable
            .s
            .iter()
            .map(|s| String::from_utf8(s.clone()).unwrap())
            .map(|s| Arc::new(s))
            .collect();
        strings
    }

    pub fn decode_string(&self, index: usize) -> Arc<String> {
        self.strings[index].clone()
    }

    pub fn decode_info(&self, info: &Option<osmpbf::Info>) -> OsmInfo {
        match info {
            Some(info) => OsmInfo {
                version: info.version.unwrap_or(0),
                timestamp: info.timestamp.unwrap_or(0),
                changeset: info.changeset.unwrap_or(0),
                uid: info.uid.unwrap_or(0) as i64,
                user_sid: info.user_sid.map_or_else(
                    || self.decode_string(0),
                    |index| self.decode_string(index as usize),
                ),
            },
            None => OsmInfo::default(),
        }
    }

    fn decode_tags(&self, keys: &[u32], values: &[u32]) -> OsmTags {
        HashMap::from_iter(zip(keys.iter(), values.iter()).map(|(key, value)| {
            (
                self.decode_string(*key as usize),
                self.decode_string(*value as usize),
            )
        }))
    }

    fn decode_dense_tags(&self, key_vals: &[i32], count: usize) -> Vec<OsmTags> {
        let mut tags: Vec<OsmTags> = Vec::with_capacity(count);
        for _ in 0..count {
            tags.push(HashMap::new());
        }
        let mut i = 0;
        let mut index = 0;
        while i < key_vals.len() {
            if key_vals[i] == 0 {
                index += 1;
                i += 1;
            } else {
                let key = self.decode_string(key_vals[i] as usize);
                let value = self.decode_string(key_vals[i + 1] as usize);
                i += 2;
                tags[index].insert(key, value);
            }
        }
        tags
    }

    pub fn decode_node(&self, node: &osmpbf::Node) -> Arc<OsmNode> {
        Arc::new(OsmNode {
            id: node.id,
            info: self.decode_info(&node.info),
            tags: self.decode_tags(&node.keys, &node.vals),
            latitude: self.decoder.latitude(node.lat),
            longitude: self.decoder.longitude(node.lon),
        })
    }

    fn decode_dense_info(&self, info: &osmpbf::DenseInfo) -> impl Iterator<Item = OsmInfo> {
        izip!(
            info.version.iter().copied().delta_decode(),
            info.timestamp.iter().copied().delta_decode(),
            info.changeset.iter().copied().delta_decode(),
            info.uid.iter().copied().delta_decode(),
            info.user_sid.iter().copied().delta_decode()
        )
        .map(|(version, timestamp, changeset, uid, user_sid)| OsmInfo {
            version: version,
            timestamp: self.decoder.timestamp(timestamp),
            changeset: changeset,
            uid: uid as i64,
            user_sid: self.decode_string(user_sid as usize),
        })
    }

    pub fn decode_dense_nodes(&self, nodes: &osmpbf::DenseNodes) -> Vec<Arc<OsmNode>> {
        let dense_info = match &nodes.denseinfo {
            Some(info) => self.decode_dense_info(info).collect(),
            None => vec![OsmInfo::default(); nodes.id.len()],
        };
        let count = nodes.id.len();
        izip!(
            nodes.id.iter().copied().delta_decode(),
            nodes.lat.iter().copied().delta_decode(),
            nodes.lon.iter().copied().delta_decode(),
            self.decode_dense_tags(&nodes.keys_vals, count),
            dense_info
        )
        .map(|(id, lat, lon, tags, info)| {
            Arc::new(OsmNode {
                id: id,
                info: info,
                tags: tags,
                latitude: self.decoder.latitude(lat),
                longitude: self.decoder.longitude(lon),
            })
        })
        .collect()
    }

    pub fn decode_way(&self, way: &osmpbf::Way) -> Arc<OsmWay> {
        Arc::new(OsmWay {
            id: way.id,
            info: self.decode_info(&way.info),
            tags: self.decode_tags(&way.keys, &way.vals),
            nodes: way.refs.iter().copied().delta_decode().collect(),
        })
    }

    fn get_relation_member_type(&self, member_type: osmpbf::relation::MemberType) -> Arc<String> {
        match member_type {
            osmpbf::relation::MemberType::Node => get_node_string(),
            osmpbf::relation::MemberType::Way => get_way_string(),
            osmpbf::relation::MemberType::Relation => get_relation_string(),
        }
    }

    fn decode_relation_members(
        &self,
        roles: &[i32],
        memmber_ids: &[i64],
        member_types: &[i32],
    ) -> Vec<OsmRelationMember> {
        izip!(
            roles,
            memmber_ids.iter().copied().delta_decode(),
            member_types
        )
        .map(|(&role, member_id, &member_type)| {
            let mtype = osmpbf::relation::MemberType::try_from(member_type).unwrap();
            OsmRelationMember {
                role: self.decode_string(role as usize),
                id: member_id,
                member_type: self.get_relation_member_type(mtype),
            }
        })
        .collect()
    }

    pub fn decode_relation(&self, relation: &osmpbf::Relation) -> Arc<OsmRelation> {
        Arc::new(OsmRelation {
            id: relation.id,
            info: self.decode_info(&relation.info),
            tags: self.decode_tags(&relation.keys, &relation.vals),
            members: self.decode_relation_members(
                &relation.roles_sid,
                &relation.memids,
                &relation.types,
            ),
        })
    }
}

pub fn decode_nodes(
    block: &osmpbf::PrimitiveBlock,
    decoder: &PrimitiveBlockDecoder,
) -> Vec<Arc<OsmNode>> {
    let mut nodes = Vec::with_capacity(10_000);
    for group in block.primitivegroup.iter() {
        for node in group.nodes.iter() {
            let data = decoder.decode_node(&node);
            nodes.push(data);
        }
        if let Some(dense) = &group.dense {
            let data = decoder.decode_dense_nodes(&dense);
            nodes.extend(data);
        }
    }
    nodes
}

pub fn decode_ways(
    block: &osmpbf::PrimitiveBlock,
    decoder: &PrimitiveBlockDecoder,
) -> Vec<Arc<OsmWay>> {
    let mut ways = Vec::with_capacity(10_000);
    for group in block.primitivegroup.iter() {
        for way in group.ways.iter() {
            let data = decoder.decode_way(&way);
            ways.push(data);
        }
    }
    ways
}

pub fn decode_relations(
    block: &osmpbf::PrimitiveBlock,
    decoder: &PrimitiveBlockDecoder,
) -> Vec<Arc<OsmRelation>> {
    let mut relations = Vec::with_capacity(10_000);
    for group in block.primitivegroup.iter() {
        for relation in group.relations.iter() {
            let data = decoder.decode_relation(&relation);
            relations.push(data);
        }
    }
    relations
}

pub fn decode_primitive_block(block: &osmpbf::PrimitiveBlock) -> OsmElements {
    let decoder = PrimitiveBlockDecoder::new(&block);
    OsmElements {
        nodes: decode_nodes(&block, &decoder),
        ways: decode_ways(&block, &decoder),
        relations: decode_relations(&block, &decoder),
    }
}
