from __future__ import annotations

import multiprocessing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tqdm import tqdm

from osmpq.osm.blob import decompress_blob
from osmpq.osm.blob import read_blob_bytes
from osmpq.osm.elements import decode_block
from osmpq.osm.types import OsmElement
from osmpq.osm.types import OsmNode
from osmpq.osm.types import OsmRelation
from osmpq.osm.types import OsmWay
from osmpq.protos.fileformat_pb2 import Blob
from osmpq.protos.osmformat_pb2 import PrimitiveBlock


@dataclass
class Stats:
    nodes: int = 0
    ways: int = 0
    relations: int = 0

    def update(self, element: OsmElement) -> None:
        if isinstance(element, OsmNode):
            self.nodes += 1
        elif isinstance(element, OsmWay):
            self.ways += 1
        elif isinstance(element, OsmRelation):
            self.relations += 1


def decoder_process(queue: multiprocessing.Queue, done: Any) -> None:
    stats = Stats()
    while done.value == 1 or not queue.empty():
        try:
            data = queue.get(timeout=1)
        except Exception:
            continue
        blob = Blob.FromString(data)
        data = decompress_blob(blob)
        block = PrimitiveBlock.FromString(data)
        for element in decode_block(block):
            stats.update(element)
        print(stats)

    print("done decode")
    print(stats)


def main():
    path = Path("/workspaces/data/osm/nevada-latest.osm.pbf")
    # path = Path("/workspaces/data/osm/us-latest.osm.pbf")

    queue = multiprocessing.Queue()
    done = multiprocessing.Value("i", 1)
    done.value = 1

    worker = [multiprocessing.Process(target=decoder_process, args=(queue, done)) for _ in range(8)]
    for w in worker:
        w.start()

    with open(path, "rb") as fin:
        with tqdm(unit_scale=True) as pbar:
            for blob in read_blob_bytes(fin):
                queue.put(blob)
                pbar.update(1)

    print("Done")

    done.value = 0
    # with open(path, "rb") as fin:
    #     nodes = 0
    #     ways = 0
    #     relations = 0
    #     with tqdm(unit_scale=True) as pbar:
    #         for blob in read_blobs(fin):
    #             item = decode_blob_data(blob)
    #             if not isinstance(item, PrimitiveBlock):
    #                 continue

    #             for element in decode_block(item):
    #                 if isinstance(element, OsmNode):
    #                     nodes += 1
    #                 elif isinstance(element, OsmWay):
    #                     ways += 1
    #                 elif isinstance(element, OsmRelation):
    #                     relations += 1
    #                 pbar.update(1)

    print("joining")
    for w in worker:
        w.join()
    print("joing done")


if __name__ == "__main__":
    main()
