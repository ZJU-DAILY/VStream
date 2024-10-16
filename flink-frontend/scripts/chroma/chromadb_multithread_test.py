import random
import string
import time
from multiprocessing import Process
from threading import Thread

import chromadb
import numpy as np
from chromadb.utils import embedding_functions
from tqdm import tqdm

# ADDRESS_FILE = "/home/auroflow/code/vector-search/VStream/flink-frontend/src/main/resources/chroma_addresses.txt"
ADDRESS_FILE = "/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/test_chroma_addresses.txt"
COLLECTION_PREFIX = "testcol_"
ROUNDS = 1000
BATCH_SIZE = 1000
EMBEDDING_SIZE = 128


def get_random_str():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))


def read_addresses(filename):
    addresses = []
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('node10'):
                host, port_low, port_high = line.strip().split(":")
                port_low = int(port_low)
                port_high = int(port_high)
                for port in range(port_low, port_high + 1):
                    addresses.append((host, port))
    return addresses


class ChromaInsert(Thread):
    def __init__(self, host, port, subtask_index, embeddings):
        super().__init__()
        self.client = chromadb.HttpClient(host, port)
        self.subtask_index = subtask_index
        collection_name = COLLECTION_PREFIX + str(subtask_index)
        collections = self.client.list_collections()
        if any(collection.name == collection_name for collection in collections):
            self.client.delete_collection(collection_name)
        metadata = {
            "hnsw:batch_size": 10000,
            "hnsw:sync_threshold": 10000,
            "hnsw:M": 16,
            "hnsw:search_ef": 16,
            "hnsw:construction_ef": 128
        }
        self.collection = self.client.create_collection(collection_name, metadata=metadata)
        self.embeddings = embeddings

    def run(self):
        start_time = time.time()
        for i in tqdm(range(ROUNDS), position=self.subtask_index, desc="Task {:3d}".format(self.subtask_index)):
            self.collection.add(ids=[str(n) for n in range(i * BATCH_SIZE, (i + 1) * BATCH_SIZE)],
                                embeddings=self.embeddings[i * BATCH_SIZE:(i + 1) * BATCH_SIZE])
        elapsed = time.time() - start_time
        print(f"ChromaInsert subtask {self.subtask_index} finished in {elapsed:.2f} s.")


if __name__ == '__main__':
    tasks = []
    embeddings = []
    ef = embedding_functions.DefaultEmbeddingFunction()
    for i in tqdm(range(ROUNDS * BATCH_SIZE)):
        embeddings.append(np.random.random(EMBEDDING_SIZE))
    for i, (host, port) in enumerate(read_addresses(ADDRESS_FILE)):
        task = ChromaInsert(host, port, i, embeddings)
        task.start()
        tasks.append(task)
    for task in tasks:
        task.join()
