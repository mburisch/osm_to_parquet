# GEMINI.md - Project Context: osm_to_parquet

## Project Overview
`osm_to_parquet` is a high-performance toolset designed to convert OpenStreetMap (OSM) Protocolbuffer Binary Format (PBF) files into Apache Parquet format. The project provides two primary implementations:

1.  **Rust Implementation (Core):** A high-performance, multi-threaded converter that directly transforms PBF files into specialized Parquet files for Nodes, Ways, and Relations. It leverages the Apache Arrow and Parquet ecosystems for efficient data processing and storage.
2.  **Python Implementation (`osmpq`):** A flexible implementation that uses `pyarrow`, `fsspec`, and `pyspark`. It supports a two-stage conversion:
    *   **PBF to Blobs:** Extracts raw PBF blobs into a Parquet dataset.
    *   **Blobs to Elements:** (Inferred) Processes the blob dataset into structured OSM elements (Nodes, Ways, Relations) using distributed computing (Spark).

### Architecture Highlights
*   **Rust Pipeline:** Uses an asynchronous reader (`tokio`) and a multi-threaded processing pipeline (`crossbeam-channel`) to parallelize blob decoding and Parquet encoding.
*   **Protobuf Handling:** Uses `prost` in Rust for PBF decoding. Python uses pre-generated `protobuf` bindings.
*   **Storage Abstraction:** Uses `object_store` in Rust to support local files, AWS S3, and HTTP sources.

## Building and Running

### Rust Tool
The Rust component is the primary CLI tool for direct conversion.

*   **Build:**
    ```bash
    cargo build --release
    ```
    *Note: Requires `protoc` installed for `prost-build` to compile OSM definitions.*

*   **Run:**
    ```bash
    cargo run --release -- --pbf-filename <path_to_pbf> --output-path <output_directory>
    ```

*   **Test:**
    ```bash
    cargo test
    ```

### Python Tool (`osmpq`)
The Python implementation is managed via `uv` or `pip`.

*   **Installation:**
    ```bash
    uv sync
    ```

*   **Usage (Example):**
    ```python
    from osmpq.pbf_converter import pbf_to_blobs
    from osmpq.parquet import WriterConfig

    pbf_to_blobs("map.osm.pbf", "output_blobs.parquet", WriterConfig())
    ```

## Development Conventions

### Code Style
*   **Rust:** Follows standard Rust idioms and `rustfmt`. Uses `edition = "2024"`.
*   **Python:** Uses `ruff` for linting and formatting. Line length is set to 120. `isort` is configured for single-line imports.

### Project Structure
*   `src/`: Rust source code.
    *   `osm/`: OSM PBF decoding and data structures.
    *   `parquet/`: Parquet schema definitions and writing logic.
    *   `processor.rs`: The main multi-threaded processing pipeline.
*   `osmpq/`: Python package source.
    *   `osm/`: Python OSM data handling.
    *   `protos/`: Pre-generated Protobuf files.
*   `protos/`: Shared `.proto` definitions for OSM format.

### Key Dependencies
*   **Rust:** `arrow`, `parquet`, `tokio`, `prost`, `object_store`, `clap`.
*   **Python:** `pyarrow`, `pyspark`, `fsspec`, `tqdm`.
