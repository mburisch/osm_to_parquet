use std::sync::Arc;

use crate::{
    osm::types::{
        ElementCount, OsmElements, OsmInfo, OsmNode, OsmRelation, OsmRelationMember, OsmTags,
        OsmWay,
    },
    parquet::schemas::{get_node_schema, get_relation_schema, get_way_schema},
};
use arrow::{
    array::{
        ArrayRef, Float64Builder, Int32Builder, Int64Builder, ListBuilder, MapBuilder, RecordBatch,
        StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, Schema},
};

struct InfoBuilder {
    version: Int32Builder,
    timestamp: Int64Builder,
    changeset: Int64Builder,
    uid: Int64Builder,
    user_sid: StringBuilder,
}

impl InfoBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            version: Int32Builder::with_capacity(capacity),
            timestamp: Int64Builder::with_capacity(capacity),
            changeset: Int64Builder::with_capacity(capacity),
            uid: Int64Builder::with_capacity(capacity),
            user_sid: StringBuilder::new(),
        }
    }

    pub fn append(&mut self, info: &OsmInfo) {
        InfoBuilder::append_optional_i32(&mut self.version, info.version);
        InfoBuilder::append_optional_i64(&mut self.timestamp, info.timestamp);
        InfoBuilder::append_optional_i64(&mut self.changeset, info.changeset);
        InfoBuilder::append_optional_i64(&mut self.uid, info.uid);
        InfoBuilder::append_optional_str(&mut self.user_sid, &info.user_sid);
    }

    fn append_optional_i32(builder: &mut Int32Builder, value: i32) {
        if value == 0 {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }

    fn append_optional_i64(builder: &mut Int64Builder, value: i64) {
        if value == 0 {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }

    fn append_optional_str(builder: &mut StringBuilder, value: &str) {
        if value.len() == 0 {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }
}

struct TagsBuilder {
    builder: MapBuilder<StringBuilder, StringBuilder>,
}

impl TagsBuilder {
    pub fn with_capacity(_capacity: usize) -> Self {
        Self {
            builder: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
        }
    }

    pub fn append(&mut self, tags: &OsmTags) {
        if tags.len() == 0 {
            self.builder.append(false).unwrap();
            return;
        }

        for (key, value) in tags {
            self.builder.keys().append_value(key.as_ref());
            self.builder.values().append_value(value.as_ref());
        }
        self.builder.append(true).unwrap();
    }

    pub fn finish(&mut self) -> ArrayRef {
        let array = self.builder.finish();
        Arc::new(array) as ArrayRef
    }
}

struct RelationMembersBuilder {
    builder: ListBuilder<StructBuilder>,
}

impl RelationMembersBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        let fields = vec![
            Field::new("role", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ];
        let member = StructBuilder::from_fields(fields, capacity);
        Self {
            builder: ListBuilder::with_capacity(member, capacity),
        }
    }

    pub fn append(&mut self, members: &[OsmRelationMember]) {
        let struct_builder = self.builder.values();

        for member in members {
            let role_builder = struct_builder.field_builder::<StringBuilder>(0).unwrap();
            role_builder.append_value(member.role.as_ref());

            let id_builder = struct_builder.field_builder::<Int64Builder>(1).unwrap();
            id_builder.append_value(member.id);

            let type_builder = struct_builder.field_builder::<StringBuilder>(2).unwrap();
            type_builder.append_value(member.member_type.as_ref());

            struct_builder.append(true);
        }
        self.builder.append(true);
    }

    pub fn finish(&mut self) -> ArrayRef {
        let array = self.builder.finish();
        Arc::new(array) as ArrayRef
    }
}

pub fn create_batch_for_nodes(nodes: &[Arc<OsmNode>], schema: Arc<Schema>) -> Option<RecordBatch> {
    if nodes.is_empty() {
        return None;
    }

    let mut id = Int64Builder::with_capacity(nodes.len());
    let mut tags = TagsBuilder::with_capacity(nodes.len());
    let mut latitude = Float64Builder::with_capacity(nodes.len());
    let mut longitude = Float64Builder::with_capacity(nodes.len());
    let mut info = InfoBuilder::with_capacity(nodes.len());

    for node in nodes {
        id.append_value(node.id);
        tags.append(&node.tags);
        latitude.append_value(node.latitude);
        longitude.append_value(node.longitude);
        info.append(&node.info);
    }

    let columns = vec![
        Arc::new(id.finish()) as ArrayRef,
        Arc::new(info.version.finish()) as ArrayRef,
        Arc::new(tags.finish()) as ArrayRef,
        Arc::new(latitude.finish()) as ArrayRef,
        Arc::new(longitude.finish()) as ArrayRef,
        Arc::new(info.timestamp.finish()) as ArrayRef,
        Arc::new(info.changeset.finish()) as ArrayRef,
        Arc::new(info.uid.finish()) as ArrayRef,
        Arc::new(info.user_sid.finish()) as ArrayRef,
    ];
    Some(RecordBatch::try_new(schema, columns).unwrap())
}

pub fn create_batch_for_ways(ways: &[Arc<OsmWay>], schema: Arc<Schema>) -> Option<RecordBatch> {
    if ways.is_empty() {
        return None;
    }

    let mut id = Int64Builder::with_capacity(ways.len());
    let mut tags = TagsBuilder::with_capacity(ways.len());
    let mut nodes = ListBuilder::with_capacity(Int64Builder::new(), ways.len());
    let mut info = InfoBuilder::with_capacity(ways.len());

    for way in ways {
        id.append_value(way.id);
        tags.append(&way.tags);
        for node in &way.nodes {
            nodes.values().append_value(*node);
        }
        nodes.append(true);
        info.append(&way.info);
    }

    let columns = vec![
        Arc::new(id.finish()) as ArrayRef,
        Arc::new(info.version.finish()) as ArrayRef,
        Arc::new(tags.finish()) as ArrayRef,
        Arc::new(nodes.finish()) as ArrayRef,
        Arc::new(info.timestamp.finish()) as ArrayRef,
        Arc::new(info.changeset.finish()) as ArrayRef,
        Arc::new(info.uid.finish()) as ArrayRef,
        Arc::new(info.user_sid.finish()) as ArrayRef,
    ];
    Some(RecordBatch::try_new(schema, columns).unwrap())
}

pub fn create_batch_for_relations(
    relations: &[Arc<OsmRelation>],
    schema: Arc<Schema>,
) -> Option<RecordBatch> {
    if relations.is_empty() {
        return None;
    }

    let mut id = Int64Builder::with_capacity(relations.len());
    let mut tags = TagsBuilder::with_capacity(relations.len());
    let mut members = RelationMembersBuilder::with_capacity(relations.len());
    let mut info = InfoBuilder::with_capacity(relations.len());

    for relation in relations {
        id.append_value(relation.id);
        tags.append(&relation.tags);
        members.append(&relation.members);
        info.append(&relation.info);
    }

    let columns = vec![
        Arc::new(id.finish()) as ArrayRef,
        Arc::new(info.version.finish()) as ArrayRef,
        Arc::new(tags.finish()) as ArrayRef,
        Arc::new(members.finish()) as ArrayRef,
        Arc::new(info.timestamp.finish()) as ArrayRef,
        Arc::new(info.changeset.finish()) as ArrayRef,
        Arc::new(info.uid.finish()) as ArrayRef,
        Arc::new(info.user_sid.finish()) as ArrayRef,
    ];
    Some(RecordBatch::try_new(schema, columns).unwrap())
}

#[derive(Debug, Clone, Default)]
pub struct ElementBatches {
    pub nodes: Option<RecordBatch>,
    pub ways: Option<RecordBatch>,
    pub relations: Option<RecordBatch>,
}

impl ElementBatches {
    pub fn from_elements(elements: &OsmElements) -> Self {
        Self {
            nodes: create_batch_for_nodes(&elements.nodes, get_node_schema()),
            ways: create_batch_for_ways(&elements.ways, get_way_schema()),
            relations: create_batch_for_relations(&elements.relations, get_relation_schema()),
        }
    }

    pub fn count(&self) -> ElementCount {
        return ElementCount {
            nodes: self.nodes.as_ref().map_or(0, |n| n.num_rows()),
            ways: self.ways.as_ref().map_or(0, |w| w.num_rows()),
            relations: self.relations.as_ref().map_or(0, |r| r.num_rows()),
        };
    }
}
