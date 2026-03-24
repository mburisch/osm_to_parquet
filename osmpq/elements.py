from __future__ import annotations

import json
import multiprocessing
from collections.abc import Callable
from typing import Any
from typing import Iterable
from typing import Protocol

import pyarrow as pa
from google.protobuf.json_format import MessageToDict
from tqdm import tqdm

from osmpq.arrow import ARROW_NODE_SCHEMA
from osmpq.arrow import ARROW_RELATION_SCHEMA
from osmpq.arrow import ARROW_WAY_SCHEMA
from osmpq.arrow import ParquetBatchWriter
from osmpq.arrow import WriterConfig
from osmpq.arrow import record_batch_for_nodes
from osmpq.arrow import record_batch_for_relations
from osmpq.arrow import record_batch_for_ways
from osmpq.osm.blob import BlobData
from osmpq.osm.blob import BlobType
from osmpq.osm.blob import decode_header_blob
from osmpq.osm.blob import decode_primtive_blob
from osmpq.osm.blob import read_pbf_file
from osmpq.osm.elements import PrimitiveBlockDecoder
from osmpq.osm.elements import decode_nodes
from osmpq.osm.elements import decode_relations
from osmpq.osm.elements import decode_ways



def write_header(output_path: str, blob_data: bytes) -> None:
    fs, base_path = pa.fs.FileSystem.from_uri(output_path)
    header = decode_header_blob(blob_data)
    message = MessageToDict(header, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)

    with fs.open_output_stream(join_path(base_path, "header.json"), compression=None) as stream:
        stream.write(json.dumps(message).encode())



def _process_worker(
    blob_queue: multiprocessing.Queue[bytes | None], output_path: str, writer_config: WriterConfig
) -> None:
    with Writer(output_path, config=writer_config) as writer:
        while True:
            blob_data = blob_queue.get()
            if blob_data is None:
                return
            writer.write_elements(blob_data)


def pbf_to_elements(blobs: Iterable[bytes]):
    for data in blobs:
        


def pbf_to_elements(pbf_filename: str, output_path: str, writer_config: WriterConfig, num_workers: int = 8) -> None:
    prepare_output_path(output_path)

    blob_queue: multiprocessing.Queue[bytes | None] = multiprocessing.Queue(maxsize=64)

    # Start worker processes
    workers = []
    for _ in range(num_workers):
        p = multiprocessing.Process(target=_process_worker, args=(blob_queue, output_path, writer_config))
        p.start()
        workers.append(p)

    # Main process: read blobs
    blobs = read_pbf_file(pbf_filename)
    blobs = tqdm(blobs, desc="Reading blobs", unit_scale=True)

    for blob in blobs:
        match blob.header.type:
            case BlobType.OSM_HEADER.value:
                write_header(output_path, blob.blob_data)
            case BlobType.OSM_DATA.value:
                blob_queue.put(blob.blob_data)

    # Send sentinels to worker processes
    for _ in range(num_workers):
        blob_queue.put(None)

    # Wait for all workers to finish
    for p in workers:
        p.join()
