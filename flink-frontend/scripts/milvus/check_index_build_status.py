from pymilvus import utility, connections

connections.connect(address="10.214.242.182:19530")
collection_name = "vector_collection"
index_name = "embedding"


def get_index_build_ratio():
    progress = utility.index_building_progress(collection_name, index_name)
    print(progress)
    ratio = progress["indexed_rows"] / progress["total_rows"]
    print("Ratio: " + str(ratio))
    return ratio


if __name__ == "__main__":
    get_index_build_ratio()
