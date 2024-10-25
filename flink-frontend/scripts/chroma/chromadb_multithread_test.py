import logging
import random
import string
import struct
import time
from multiprocessing import Process
from threading import Thread

import chromadb
import numpy as np
from chromadb.api import CollectionConfiguration
from chromadb.api.configuration import HNSWConfiguration
from chromadb.utils import embedding_functions
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction
from tqdm import tqdm

ADDRESS_FILE = "/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/chroma_addresses.txt"
DATA_FILE = "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs"
COLLECTION_PREFIX = "testcol2_"
ROUNDS = 6250
GEN_ROUNDS = 1000
BATCH_SIZE = 1000
EMBEDDING_SIZE = 128


class FvecIterator:
    class InputType:
        F_VEC = 'F_VEC'
        B_VEC = 'B_VEC'

    def __init__(self, filename, num_loops=1, skip=0, limit=float('inf')):
        self.logger = logging.getLogger(__name__)
        self.file = open(filename, 'rb')
        self.num_loops = num_loops
        self.loop = 0
        self.length = self._get_file_length()
        self.input_type = self._determine_input_type(filename)
        self.limit = limit
        self.count = 0

        dimension = self._read_int_little_endian()
        vector_width = 4 + dimension * (4 if self.input_type == self.InputType.F_VEC else 1)
        self.start_position = vector_width * skip
        self.file.seek(self.start_position)

    def __iter__(self):
        return self

    def __next__(self) -> list[float]:
        if self._is_eof():
            if self.loop < self.num_loops:
                self.logger.debug("File reached limit, seek to start pos")
                self.file.seek(self.start_position)
                self.loop += 1
            else:
                raise StopIteration

        self.count += 1
        dimension = self._read_int_little_endian()

        if self.input_type == self.InputType.F_VEC:
            vector = struct.unpack('<' + 'f' * dimension, self.file.read(dimension * 4))
            return vector
        elif self.input_type == self.InputType.B_VEC:
            vector = self.file.read(dimension)
            return [float(b) for b in vector]
        else:
            raise RuntimeError("Impossible branch.")

    def _is_eof(self):
        return (self.count != 0 and self.count % self.limit == 0) or self.file.tell() == self.length

    def _read_int_little_endian(self) -> int:
        return struct.unpack('<I', self.file.read(4))[0]

    def _get_file_length(self):
        self.file.seek(0, 2)  # Move to end of file
        length = self.file.tell()
        self.file.seek(0)  # Move back to start
        return length

    def _determine_input_type(self, filename):
        if filename.endswith('.fvecs'):
            return self.InputType.F_VEC
        elif filename.endswith('.bvecs'):
            return self.InputType.B_VEC
        else:
            raise RuntimeError("Unknown file type.")


class Ring:
    def __init__(self, data):
        self.data = data

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = key.start % len(self.data)
            stop = key.stop % len(self.data)
            if start < stop:
                return self.data[start:stop]
            else:
                return self.data[start:] + self.data[:stop]
        else:
            return self.data[key % len(self.data)]


def get_random_str():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))


def read_addresses(filename):
    addresses = []
    with open(filename, 'r') as f:
        for line in f:
            host, port_low, port_high = line.strip().split(":")
            port_low = int(port_low)
            port_high = int(port_high)
            for port in range(port_low, port_high + 1):
                addresses.append((host, port))
    return [('node193', 8000)]
    # return addresses


class ChromaInsert(Thread):
    def __init__(self, host, port, subtask_index, embeddings):
        super().__init__()
        self.client = chromadb.HttpClient(host, port)
        self.subtask_index = subtask_index
        self.collection_name = COLLECTION_PREFIX + str(subtask_index)
        collections = self.client.list_collections()
        if any(collection.name == self.collection_name for collection in collections):
            self.client.delete_collection(self.collection_name)
        metadata = {
            "hnsw:batch_size": 10000,
            "hnsw:sync_threshold": 100000,
            "hnsw:M": 16,
            "hnsw:search_ef": 16,
            "hnsw:construction_ef": 128,
            "hnsw:num_threads": 16,
            "hnsw:space": "l2",
            "hnsw:resize_factor": 1.2
        }
        self.collection = self.client.create_collection(self.collection_name, metadata=metadata,
                                                        embedding_function=DefaultEmbeddingFunction())
        self.embeddings = Ring(embeddings)
        print('Collection ' + self.collection_name + ' created.')

    def run(self):
        pass
        # start_time = time.time()
        # total = 0
        # for r in tqdm(range(ROUNDS), position=self.subtask_index, desc="Task {:3d}".format(self.subtask_index)):
        #     self.collection.add(ids=[str(n) for n in range(r * BATCH_SIZE, (r + 1) * BATCH_SIZE)],
        #                         embeddings=self.embeddings[r * BATCH_SIZE:(r + 1) * BATCH_SIZE])
        #     total += BATCH_SIZE
        #     if total % 100000 == 0:
        #         now = time.time()
        #         elapsed = now - start_time
        #         print(f"{elapsed}s: {self.collection_name} inserted {total} vectors.")
        #         start_time = now


if __name__ == '__main__':
    vectors = []
    # it = FvecIterator(DATA_FILE)
    # for vector in it:
    #     vectors.append(vector)

    # print('vector sample: ' + str(vectors[255]))

    tasks = []
    for i, (host, port) in enumerate(read_addresses(ADDRESS_FILE)):
        task = ChromaInsert(host, port, i, vectors)
        task.start()
        tasks.append(task)
    for task in tasks:
        task.join()
