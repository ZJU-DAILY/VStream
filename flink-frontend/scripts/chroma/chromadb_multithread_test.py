import random
import string
import time
from multiprocessing import Process

import chromadb
import numpy as np
from chromadb.utils import embedding_functions
from tqdm import tqdm

HOST = "localhost"
PORTS = [8000, 8000]
COLLECTION_PREFIX = "testcol_"
PARALLELISM = 2
ROUNDS = 500
BATCH_SIZE = 100
EMBEDDING_SIZE = 384

def get_random_str():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))


class ChromaInsert(Process):
    def __init__(self, subtask_index, port):
        super().__init__()
        self.client = chromadb.HttpClient(HOST, port)
        self.subtask_index = subtask_index
        collection_name = COLLECTION_PREFIX + str(subtask_index)
        collections = self.client.list_collections()
        if any(collection.name == collection_name for collection in collections):
            self.client.delete_collection(collection_name)
        self.collection = self.client.create_collection(collection_name)

    def run(self):
        embeddings = []
        ef = embedding_functions.DefaultEmbeddingFunction()
        for i in tqdm(range(ROUNDS * BATCH_SIZE)):
            embeddings.append(np.random.random(EMBEDDING_SIZE))
        print(f"ChromaInsert subtask {self.subtask_index} started")
        start_time = time.time()
        for i in tqdm(range(ROUNDS), position=self.subtask_index):
            self.collection.add(ids=[str(n) for n in range(i * BATCH_SIZE, (i + 1) * BATCH_SIZE)],
                                embeddings=embeddings[i * BATCH_SIZE:(i + 1) * BATCH_SIZE])
        elapsed = time.time() - start_time
        print(f"ChromaInsert subtask {self.subtask_index} finished in {elapsed:.2f} s.")


if __name__ == '__main__':
    tasks = []
    for i in range(PARALLELISM):
        chroma_insert = ChromaInsert(i, PORTS[i])
        chroma_insert.start()
        tasks.append(chroma_insert)
    for task in tasks:
        task.join()
