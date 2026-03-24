from __future__ import annotations

from typing import Iterable

from tqdm import tqdm

from osmpq.arrow import record_batch_for_nodes
from osmpq.arrow import record_batch_for_relations
from osmpq.arrow import record_batch_for_ways
from osmpq.blobs import from_record_batch
from osmpq.blobs import header_extractor
from osmpq.io import ElementBatch
from osmpq.io import ElementsWriter
from osmpq.io import FileTemplates
from osmpq.io import WriterConfig
from osmpq.io import clear_output_path
from osmpq.io import create_output_path
from osmpq.io import read_blobs_from_parquet
from osmpq.io import read_blobs_from_pbf
from osmpq.osm.blob import BlobData
from osmpq.osm.blob import BlobType
from osmpq.osm.blob import decode_primtive_blob
from osmpq.osm.elements import PrimitiveBlockDecoder
from osmpq.osm.elements import decode_nodes
from osmpq.osm.elements import decode_relations
from osmpq.osm.elements import decode_ways


def to_record_batch(blob_data: bytes) -> ElementBatch:
    block = decode_primtive_blob(bytes(blob_data))
    decoder = PrimitiveBlockDecoder(block)

    nodes = record_batch_for_nodes(list(decode_nodes(decoder)))
    ways = record_batch_for_ways(list(decode_ways(decoder)))
    relations = record_batch_for_relations(list(decode_relations(decoder)))

    return ElementBatch(nodes=nodes, ways=ways, relations=relations)


def to_record_batches(blobs: Iterable[BlobData]) -> Iterable[ElementBatch]:
    for blob_data in blobs:
        if blob_data.header.type == BlobType.OSM_DATA:
            yield to_record_batch(blob_data.blob_data)


def to_elements_parquet(
    blobs: Iterable[BlobData],
    writer: ElementsWriter,
) -> None:
    blobs = tqdm(blobs, desc="Processing blobs", unit_scale=True)
    batches = to_record_batches(blobs)
    with writer:
        for batch in batches:
            writer.write(batch)


def pbf_to_elements_parquet(
    pbf_filename: str,
    output_path: str,
    header_output_filename: str | None,
    writer_config: WriterConfig,
    file_templates: FileTemplates | None = None,
) -> None:
    clear_output_path(output_path)
    blobs = read_blobs_from_pbf(pbf_filename)
    if header_output_filename is not None:
        blobs = header_extractor(blobs, header_output_filename)

    writer = ElementsWriter(
        path=output_path,
        config=writer_config,
        file_templates=file_templates,
    )
    to_elements_parquet(blobs, writer)


def pbf_parquet_to_elements_parquet(
    blob_parquet_filename: str,
    output_path: str,
    writer_config: WriterConfig,
    file_templates: FileTemplates | None = None,
) -> None:
    create_output_path(output_path)
    batches = read_blobs_from_parquet(blob_parquet_filename)
    blobs = (blob for batch in batches for blob in from_record_batch(batch))
    writer = ElementsWriter(
        path=output_path,
        config=writer_config,
        file_templates=file_templates or FileTemplates.for_hash(blob_parquet_filename),
    )
    to_elements_parquet(blobs, writer)
