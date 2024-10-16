echo "note: ulimit is set to $(ulimit -n)"
source "$(dirname -- ${BASH_SOURCE})"/set_env.sh
nodes="node10 node11 node12 node13 node14 node15 node182 node21 node22 node23"


# Backup params.yaml
cp /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml ./params/params.yaml.bak

# Find all params.yaml files in the params directory
for file in $(find ./params -name "*.yaml" | sort -n); do

  # if there is 'no-compress' in the filename, we should change the ROCKSDB_PATH in ~/.bashrc on all machines
  if [[ $file == *"no-compress"* ]]; then
    echo "no-compress found in filename. Changing ROCKSDB_PATH to /build-no-compress on all machines..."
    for node in $nodes; do
      # change "VectorBackend-RocksDB/build\n" to "VectorBackend-RocksDB/build-no-compress\n" in ~/.bashrc
      ssh $node "sed -i 's/VectorBackend-RocksDB\/build$/VectorBackend-RocksDB\/build-no-compress/g' /home/auroflow/.bashrc"
    done
  else
    echo "no-compress not found in filename. Changing ROCKSDB_PATH to /build on all machines..."
    for node in $nodes; do
      # change "VectorBackend-RocksDB/build-no-compress\n" to "VectorBackend-RocksDB/build\n" in ~/.bashrc
      # if it is already the case, do not change
      ssh $node "sed -i 's/VectorBackend-RocksDB\/build-no-compress$/VectorBackend-RocksDB\/build/g' /home/auroflow/.bashrc"
    done
  fi


  DATE=$(date | tr -d \\n)

  # Copy params.yaml to the project directory
  cp $file /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml
  
  # Run the job, and save the syslogs folder name (last 9 characters)
  # if filename contains 'hnswlib', invoke start-hnswlib.sh
  if [[ $file == *"hnswlib"* ]]; then
    echo "Running hnswlib job..."
    folder=$(./start-hnswlib.sh -f | grep "Syslogs folder name" | awk 'NF{ print $NF }')
    bn=$(basename $folder)
  else
    echo "Running rocksdb job..."
    folder=$(./start.sh -f | grep "Syslogs folder name" | awk 'NF{ print $NF }')
    bn=$(basename $folder)
  fi

  # Get the exit code of check-flink-task-manager.py
  /usr/bin/python3 check-flink-task-manager.py
  if [ $? -ne 0 ]; then
    echo "node10 should not be the partitioner. Terminating..."
    ./stop.sh $bn -a
  else
    PROMPT='rocksdb: Running '$file'. Syslogs folder: '$bn''
    echo $PROMPT

    # Dingtalk notification
    curl "$DINGTALK_NOTIFICATION_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"msgtype\": \"text\", \"text\": {\"content\": \"$PROMPT\"}}" &> /dev/null
    
    sleep 10m
    /usr/bin/python3 wait-flink-job.py $bn
    status=$?
    ./stop.sh $bn
    if [ $status -ne 0 ]; then
      mv $file "$file.failed"
    else
      mv $file "$file.done"
    fi
  fi
done

cp ./params/params.yaml.bak /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml

# if there is still '.yaml' file, rerun
if [ -f ./params/*.yaml ]; then
  echo "Rerunning..."
  ./run-all.sh
fi
