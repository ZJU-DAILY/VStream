from pymilvus import utility, connections

connections.connect(address="10.214.242.182:19530")
collection_name = "vector_collection"
index_name = "embedding"

if __name__ == "__main__":
    process = utility.index_building_progress(collection_name, index_name)
    print(process)
    print(f"Ratio: {process['indexed_rows'] / process['total_rows']}")
