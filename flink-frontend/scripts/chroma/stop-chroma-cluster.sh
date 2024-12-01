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

# Read each line from the file
while IFS=: read -r host port_low port_high; do
  # Batch size is 5
  for i in $(seq "$port_low" 5 "$port_high"); do
    # concatenate through i to min(i+4, port_high)
    NAMES=""
    for j in $(seq "$i" $(($i+4))); do
      if [ "$j" -gt "$port_high" ]; then
        break
      fi
      NAMES="$NAMES chromadb_"$host"_$j"
    done
    echo $NAMES
    # SSH into the host and stop the chroma server
    docker -H ssh://"$USER"@"$host":4399 stop $NAMES
  done
done < "$FILE"
echo "done"