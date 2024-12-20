#!/bin/bash
# File containing host:port pairs
USER="auroflow"
SSH_PORT="4399"
if [ "$1" == "test" ]; then
  FILE="/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/chroma_addresses.txt"
  ROOT="root"
elif [ "$1" == "prod" ]; then
  FILE="/home/auroflow/code/vector-search/VStream/flink-frontend/src/main/resources/chroma_addresses.txt"
  ROOT="root"
else
  echo "Usage: start-chroma-cluster.sh <test|prod>"
  exit 1
fi
CHROMA_PERSISTENT_PATH="/home/auroflow/storage/chroma"
CPU_LIMIT="1.4"  # unused

# Read each line from the file
while IFS=: read -r host port_low port_high; do
  # SSH into the host and start the chroma server
  for port in $(seq "$port_low" "$port_high"); do
    echo "Clearing chroma data and starting chroma server on $host:$port"
    ssh -p $SSH_PORT -n $ROOT@"$host" "rm -rf $CHROMA_PERSISTENT_PATH/chroma_""$host"_"$port"
    docker -H ssh://"$USER"@"$host":$SSH_PORT run -d --rm --name chromadb_"$host"_"$port" -p "$port":8000 -v "$CHROMA_PERSISTENT_PATH"/chroma_"$host"_"$port":/chroma/chroma -e IS_PERSISTENT=TRUE -e ANONYMIZED_TELEMETRY=FALSE chromadb/chroma:0.5.12
  done
done < "$FILE"
echo "done"
