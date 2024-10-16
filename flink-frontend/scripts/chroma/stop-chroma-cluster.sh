#!/bin/bash
# File containing host:port pairs
USER="auroflow"
#FILE="/home/auroflow/code/vector-search/VStream/flink-frontend/src/main/resources/chroma_addresses.txt"
FILE="/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/test_chroma_addresses.txt"

# Read each line from the file
while IFS=: read -r host port_low port_high; do
  # SSH into the host and stop the chroma server
  for port in $(seq "$port_low" "$port_high"); do
    docker -H ssh://"$USER"@"$host":4399 stop chromadb_"$host"_"$port"
  done
done < "$FILE"
echo "done"