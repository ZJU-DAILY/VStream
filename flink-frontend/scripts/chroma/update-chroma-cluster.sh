#!/bin/bash
# File containing host:port pairs
USER="auroflow"
if [ "$1" == "test" ]; then
  FILE="/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/chroma_addresses.txt"
elif [ "$1" == "prod" ]; then
  FILE="/home/auroflow/code/vector-search/VStream/flink-frontend/src/main/resources/chroma_addresses.txt"
else
  echo "Usage: start-chroma-cluster.sh <test|prod>"
  exit 1
fi

CPUS="1.4"

# Read each line from the file
while IFS=: read -r host port_low port_high; do
  # concatenate
  NAMES=$(seq "$port_low" "$port_high" | xargs -I {} echo -n "chromadb_"$host"_{} ")
  echo $NAMES
  # SSH into the host and stop the chroma server
  docker -H ssh://"$USER"@"$host":4399 update --cpus $CPUS $NAMES
done < "$FILE"
echo "done"