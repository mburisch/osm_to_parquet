from __future__ import annotations

import argparse

from osmpq.pbf_converter import ProcessorConfig
from osmpq.pbf_converter import pbf_to_blobs


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert OSM PBF to Parquet")
    parser.add_argument("--pbf_filename", type=str, help="Path to the OSM PBF file")
    parser.add_argument("--output_path", type=str, help="Path to the output directory")
    return parser


def get_processor_config(args: argparse.Namespace) -> ProcessorConfig:
    return ProcessorConfig()


def main():
    parser = create_parser()
    args = parser.parse_args()

    config = get_processor_config(args)
    pbf_to_blobs(args.pbf_filename, args.output_path, config)


if __name__ == "__main__":
    main()
