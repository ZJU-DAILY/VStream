# VStream: A Distributed Streaming Vector Search System

## Introduction

VStream is a distributed streaming vector search system with the following features:

- Vector search on streaming data
- Dynamic data partitioning
- Hierarchical storage mechanism
- Vector compression
- Hot-cold separation

## Build & Run

### Requirements

- Linux
- gcc >= 11
- cmake >= 3.10
- Java 8
- Maven >= 3.8.6
- Flink 1.18

### Build

```bash
bash build.sh
```

After building, make sure to do the following on every machine in your Flink cluster:

- Put `build/java/librocksdbjni-shared.so` in `$LD_LIBRARY_PATH` and rename it to `librocksdbjni.so`.
- Copy `build/java/rocksdbjni_classes.jar` to `$FLINK_HOME/lib` directory.

### Data Preparation

Before running the experiments, you should upload the vector dataset to HDFS. The vector dataset is expected
to be in SIFT format.

### Configurations

An example of the configuration file is given in `flink-frontend/src/main/resources/params.yaml`, which contains 
runtime parameters related to HDFS, RocksDB, the HNSW index and the Flink job. For meaning of each parameter, see 
`flink-frontend/src/main/java/cn/edu/zju/daily/util/Parameters.java`.

### Run

Run the experiment pipeline by submitting the Flink job:

```bash
flink run -c cn.edu.zju.daily.VStreamSearchJob ./build/flink-frontend/vstream-1.1.jar <params.yaml>
```

where `params.yaml` is the configuration file.

## Comparing with the Baselines

This repo contains the baseline solution using Milvus, Qdrant and Chroma. 

### Milvus

\1. Start a Milvus 2.3 cluster.

\2. Fill the Milvus root information in the configuration file.

\3. Run:

```bash
flink run -c cn.edu.zju.daily.MilvusSeparatedStreamSearchJob ./build/flink-frontend/vstream-1.1.jar <params.yaml>
```

### Qdrant

\1. Start a Qdrant 1.12.1 cluster.

\2. Fill the Qdrant-related parameters in the configuration file.

\3. Run:

```bash
flink run -c cn.edu.zju.daily.QdrantSeparatedStreamSearchJob ./build/flink-frontend/vstream-1.1.jar <params.yaml>
```

### Chroma

\1. Start a Chroma instance (version 0.5.12) for each Flink parallelism on the local machine. You can use this script:

```
flink-frontend/scripts/chroma/start-chroma-cluster.sh
```

\2. Fill the Chroma-related parameters in the configuration file.

\3. Run:

```bash
flink run -c cn.edu.zju.daily.ChromaSeparatedStreamSearchJob ./build/flink-frontend/vstream-1.1.jar <params.yaml>
```

## Notice

This system uses code from RocksDB and Apache Flink, both licensed under Apache 2.0 License.  
