# Tools to convert OSM pbf files to parquet

## Python
* Split a pbf file into smaller pbf files
* Convert a pbf file to parquet with nodes, ways, and relations

When splitting a pbf file the header blob is duplicated into all files.

For best efficiency split the OSM pbf into smaller files to later convert them in parallel.
Once the pbf file is split into smaller parts, use multiple machines (or processes) to
extract the elements (nodes, ways, relations) and store as Parquet files.

Splitting the pbf files is generally I/O limited as it does very little actual processing.
Processing a pbf file is compute limited, so it will profit from parallelization. 

Both splitting and converting use fsspec for I/O, so you can (and should) read and write directly
to cloud storage.

Convert PBF to Parquet file
```bash
osmpq-pbf-extract split \
    --pbf-filename path_to_pbf.pbf \
    --output-path pbf_output_folder
```

In parallel for every Parquet file call
```bash
osmpq-pbf-extract elements \
    --pbf-filename pbf_filename.pbf \
    --output-path parquet_output_folder
```

The output folder will have nodes/, ways/, and relations/ subfolders containing the Parquet files.

## Rust
Convert a pbf file to parquets for nodes, ways, relations
