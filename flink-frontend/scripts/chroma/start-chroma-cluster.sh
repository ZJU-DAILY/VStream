#!/bin/bash
# File containing host:port pairs
USER="auroflow"
FILE="/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/test_chroma_addresses.txt"
CHROMA_PERSISTENT_PATH="/home/auroflow/chroma"
CPU_LIMIT="1"

# Read each line from the file
while IFS=: read -r host port; do
  # SSH into the host and start the chroma server
  docker -H ssh://"$USER"@"$host":4399 run -d --rm --name chromadb_"$host"_"$port" -p "$port":8000 -v "$CHROMA_PERSISTENT_PATH"/chroma_"$host"_"$port":/chroma/chroma -e IS_PERSISTENT=TRUE -e ANONYMIZED_TELEMETRY=FALSE --cpus $CPU_LIMIT chromadb/chroma:0.5.11
done < "$FILE"
echo "done"
