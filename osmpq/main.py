from __future__ import annotations

import multiprocessing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tqdm import tqdm

from osmpq.osm.blob import decode_primtive_blob
from osmpq.osm.blob import read_blobs
from osmpq.osm.elements import decode_block
from osmpq.osm.types import OsmElement
from osmpq.osm.types import OsmNode
from osmpq.osm.types import OsmRelation
from osmpq.osm.types import OsmWay


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

        block = decode_primtive_blob(data)
        for element in decode_block(block):
            stats.update(element)

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
            for blob in read_blobs(fin):
                if blob.header.type == "OSMHeader":
                    continue
                queue.put(blob.data)
                pbar.update(1)

    print("Done")

    done.value = 0

    for w in worker:
        w.join()


if __name__ == "__main__":
    main()
