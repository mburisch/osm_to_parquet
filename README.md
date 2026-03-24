# Tools to convert OSM pbf files to parquet

## Python
* Convert a pbf file to a parquet where each row is a blob.
* Convert a parquet with blobs to nodes, ways, and relation parquets.
* Convert a pbf file to parquet with nodes, ways, and relations directly.

For best efficiency convert the OSM pbf to blobs first, as pbf cannot be easily read
in parallel. Therefore, the pbf should be read directly from the cloud (e.g. http, s3, gcs)
and written to the cloud. Then in a next step using multiple machines (e.g. Spark) the blob parquet
files should be read and converted to actual OSM elements (nodes, ways, relations).

Convert PBF to Parquet file
```bash
pbf_extract blobs --pbf-filename path_to_pbf.pbf --output-path blobs_output_folder --header-output-filename option_header_path.json
```

In parallel for every Parquet file call
```bash
pbf_extract parquet-elements --blob-parquet-filename path_to_one_blob_parquet_file.parquet --output-path elements_output_folder
```

The output folder will have nodes/, ways/, and relations/ subfolders with resulting parquet files.

## Rust
Convert a pbf file to parquets for nodes, ways, relations
