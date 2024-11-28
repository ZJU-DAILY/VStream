#!/bin/bash
SCRIPT_DIR=$(dirname -- "${BASH_SOURCE}")
source "$SCRIPT_DIR/set_env.sh"
echo "note: ulimit is set to $(ulimit -n)"
source "$(dirname -- ${BASH_SOURCE})"/set_env.sh
FLINK_FRONTEND_DIR=/home/auroflow/code/vector-search/VStream/flink-frontend
COMPLETED_LOG="$FLINK_FRONTEND_DIR/params/completed"

 nodes="node10 node11 node12 node13 node14 node15 node182 node21 node22 node23"

# Find all params.yaml files in the params directory
for file in $(find "$FLINK_FRONTEND_DIR/params" -maxdepth 1 -name "*.yaml" | sort -n); do
  base=$(basename "$file")
  # For each line in the completed log, check if the file has been processed
  while IFS=" " read -r status filename; do
    if [ "$filename" == "$base" ]; then
      echo "Skipping $status $base"
      continue 2
    fi
  done < "$COMPLETED_LOG"
  
   # if there is 'no-compress' in the filename, we should change the ROCKSDB_PATH in ~/.bashrc on all machines
   if [[ $file == *"no-compress"* ]]; then
     echo "no-compress found in filename. Changing ROCKSDB_PATH to /build-no-compress on all machines..."
     for node in $nodes; do
       # change "VectorBackend-RocksDB/build\n" to "VectorBackend-RocksDB/build-no-compress\n" in ~/.bashrc
       ssh $node "sed -i 's/VStream\/build$/VStream\/build-no-compress/g' /home/auroflow/.bashrc"
     done
   else
     echo "no-compress not found in filename. Changing ROCKSDB_PATH to /build on all machines..."
     for node in $nodes; do
       # change "VectorBackend-RocksDB/build-no-compress\n" to "VectorBackend-RocksDB/build\n" in ~/.bashrc
       # if it is already the case, do not change
       ssh $node "sed -i 's/VStream\/build-no-compress$/VStream\/build/g' /home/auroflow/.bashrc"
     done
   fi
  if [[ $file == *"vstream"* ]]; then
    echo "Running vstream job: $file"
    
    "$SCRIPT_DIR/start-vstream.sh" -p "$file" -f true
    syslog_dir=$(cat ./.syslog_dir)
    syslog_dirname=$(basename "$syslog_dir")
    
    PROMPT='rocksdb: Running "'$file'". Syslogs folder: '$syslog_dirname''
    echo "$PROMPT"
    
    # Dingtalk notification
    curl "$DINGTALK_NOTIFICATION_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"msgtype\": \"text\", \"text\": {\"content\": \"$PROMPT\"}}" &> /dev/null
    
    sleep 2m
    
    python3 "$SCRIPT_DIR/wait-flink-job.py" "$syslog_dirname"
    status=$?
    "$SCRIPT_DIR/stop-vstream.sh" -f "$syslog_dirname" -a y
    rm -f ./.syslog_dir
    if [ $status -ne 0 ]; then
      echo "fail $base" >> "$COMPLETED_LOG"
    else
      echo "done $base" >> "$COMPLETED_LOG"
    fi
  else
    echo "Skip non-vstream job $base."
    continue
  fi
done
