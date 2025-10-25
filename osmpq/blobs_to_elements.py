from __future__ import annotations

import argparse

from osmpq.element_converter import blobs_to_elements
from osmpq.spark import local_spark


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert OSM blob parquet to element parquet")
    parser.add_argument("--input_path", type=str, help="Path to the OSM PBF source parquets")
    parser.add_argument("--output_path", type=str, help="Path to the output directory")
    return parser


def main():
    # path = "/workspaces/data/osm/nevada-latest.osm.pbf"
    # path = "/workspaces/data/osm/us-latest.osm.pbf"
    # pbf_to_parquet(path, "/tmp/parquet/")
    parser = create_parser()
    args = parser.parse_args()

    # config = {
    #     "spark.driver.memory": "8g",
    #     "spark.executor.memory": "8g",
    # }
    config = {}

    with local_spark(config) as spark_session:
        blobs = spark_session.read.parquet(args.input_path)
        blobs = blobs.limit(100)
        blobs_to_elements(blobs, args.output_path)


if __name__ == "__main__":
    main()
