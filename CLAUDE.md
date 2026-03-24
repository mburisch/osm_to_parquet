# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Converts OpenStreetMap PBF files to Apache Parquet format. Two implementations:
- **Rust (primary):** Multi-threaded pipeline using tokio async I/O, crossbeam channels, and arrow/parquet crates
- **Python (`osmpq/`):** Uses pyarrow, fsspec, protobuf; supports PBF-to-blobs and blobs-to-elements conversion

## Build & Run

### Rust
```bash
cargo build --release          # requires protoc installed for prost-build
cargo run --release -- --pbf-filename <path> --output-path <output_dir>
cargo test
```

### Python
```bash
uv sync                        # install dependencies
uv run ruff check osmpq/       # lint
uv run ruff format osmpq/      # format
uv run mypy osmpq/             # type check
```

## Architecture (Rust)

The processing pipeline has 4 stages connected by bounded crossbeam channels:

1. **PBF Reader** (`src/osm/pbf.rs`) — async reads PBF blobs from local/S3/HTTP via `object_store`
2. **Blob Decoder** (`src/osm/blobs.rs`, `src/osm/elements.rs`) — N threads decompress blobs (zlib/lz4/lzma/zstd) and decode protobuf into OSM elements
3. **Parquet Encoder** (`src/parquet/`) — N threads convert elements into Arrow record batches and serialize to in-memory Parquet files
4. **File Writer** (`src/io.rs`) — async writes Parquet files to output via `object_store` (local, S3, HTTP)

Output is split into `nodes/`, `ways/`, `relations/` subdirectories with sequentially numbered Parquet files.

### Key modules
- `src/osm/types.rs` — OSM domain types (Node, Way, Relation, Tag, etc.)
- `src/parquet/schemas.rs` — Arrow schema definitions for each element type
- `src/parquet/records.rs` — Conversion from OSM types to Arrow arrays
- `src/parquet/writer.rs` — Streaming Parquet writer with configurable flush thresholds
- `src/progress.rs` — Progress tracking trait with console implementation (indicatif)
- `src/processor.rs` — Pipeline stage functions wiring the stages together
- `build.rs` — Compiles `protos/*.proto` via prost-build; generated code included in `src/lib.rs`

### Protobuf
Proto definitions are in `protos/` (fileformat.proto, osmformat.proto). The build script generates Rust code at compile time. Python uses pre-generated bindings in `osmpq/protos/`.

## Conventions

- Rust edition 2024
- Python: ruff with line-length 120, isort single-line imports, ignore F401
- Python requires Python >= 3.14
