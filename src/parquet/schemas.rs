use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, Schema};

use crate::osm::types::{OsmNode, OsmRelation, OsmWay};

fn get_tags_field() -> Field {
    Field::new_map(
        "tags",
        "entries",
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Utf8, true),
        false,
        true,
    )
}

fn get_relation_member_field() -> Field {
    Field::new_struct(
        "item",
        vec![
            Field::new("role", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("type", DataType::Utf8, false),
        ],
        true,
    )
}

pub fn create_node_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("version", DataType::Int32, true),
        get_tags_field(),
        Field::new("latitude", DataType::Float64, false),
        Field::new("longitude", DataType::Float64, false),
        Field::new("timestamp", DataType::Int64, true),
        Field::new("changeset", DataType::Int64, true),
        Field::new("uid", DataType::Int64, true),
        Field::new("user_sid", DataType::Utf8, true),
    ];
    Arc::new(Schema::new(fields))
}

pub fn create_way_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("version", DataType::Int32, true),
        get_tags_field(),
        Field::new_list("nodes", Field::new_list_field(DataType::Int64, true), true),
        Field::new("timestamp", DataType::Int64, true),
        Field::new("changeset", DataType::Int64, true),
        Field::new("uid", DataType::Int64, true),
        Field::new("user_sid", DataType::Utf8, true),
    ];
    Arc::new(Schema::new(fields))
}

pub fn create_relation_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("version", DataType::Int32, true),
        get_tags_field(),
        Field::new_list("members", get_relation_member_field(), true),
        Field::new("timestamp", DataType::Int64, true),
        Field::new("changeset", DataType::Int64, true),
        Field::new("uid", DataType::Int64, true),
        Field::new("user_sid", DataType::Utf8, true),
    ];
    Arc::new(Schema::new(fields))
}

pub fn get_node_schema() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    let s = SCHEMA.get_or_init(|| create_node_schema());
    s.clone()
}

pub fn get_way_schema() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    let s = SCHEMA.get_or_init(|| create_way_schema());
    s.clone()
}

pub fn get_relation_schema() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    let s = SCHEMA.get_or_init(|| create_relation_schema());
    s.clone()
}

pub trait ParquetSchema {
    fn get_schema() -> Arc<Schema>;
}

impl ParquetSchema for OsmNode {
    fn get_schema() -> Arc<Schema> {
        crate::parquet::schemas::get_node_schema()
    }
}

impl ParquetSchema for OsmWay {
    fn get_schema() -> Arc<Schema> {
        crate::parquet::schemas::get_way_schema()
    }
}

impl ParquetSchema for OsmRelation {
    fn get_schema() -> Arc<Schema> {
        crate::parquet::schemas::get_relation_schema()
    }
}
