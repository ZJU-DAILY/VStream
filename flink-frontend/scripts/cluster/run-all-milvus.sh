#!/bin/bash

SCRIPT_DIR=$(dirname -- "${BASH_SOURCE}")
source "$SCRIPT_DIR/set_env.sh"
FLINK_FRONTEND_DIR=/home/auroflow/code/vector-search/VStream/flink-frontend
COMPLETED_LOG="$FLINK_FRONTEND_DIR/params/completed"
MILVUS_ROOT="node182"
SSH_CONFIG="-p 4399"

# Find all params.yaml files in the params directory
for file in $(find "$FLINK_FRONTEND_DIR/params" -name "*.yaml" | sort -n); do
  base=$(basename "$file")
  # For each line in the completed log, check if the file has been processed
  while IFS=" " read -r status filename; do
    if [ "$filename" == "$base" ]; then
      echo "Skipping $status $base"
      continue 2
    fi
  done < "$COMPLETED_LOG"

  # Run the job, and save the syslogs folder name (last 9 characters)
  # if filename contains 'hnswlib', invoke start-hnswlib.sh
  if [[ $file == *"milvus"* ]]; then
    echo "Running milvus job: $file"

    # Restart milvus cluster
    ssh $SSH_CONFIG $MILVUS_ROOT "cd /home/auroflow/milvus/milvus-cluster-deploy-script/ && ./stop-cluster.sh"
    echo "Milvus cluster stopped."
    ssh $SSH_CONFIG root@$MILVUS_ROOT "rm -rf /home/auroflow/milvus/milvus-cluster-deploy-script/volumes"
    echo "Milvus data removed."
    ssh $SSH_CONFIG $MILVUS_ROOT "cd /home/auroflow/milvus/milvus-cluster-deploy-script/ && ./run-cluster.sh"
    echo "Milvus cluster started."

    "$SCRIPT_DIR/start-milvus.sh" -p "$file" -f true
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
    "$SCRIPT_DIR/stop-milvus.sh" -f "$syslog_dirname" -a y
    rm -f ./.syslog_dir
    if [ $status -ne 0 ]; then
      echo "fail $base" >> "$COMPLETED_LOG"
    else
      echo "done $base" >> "$COMPLETED_LOG"
    fi
  else
    echo "Skip non-chroma job."
    echo "skip $base" >> "$COMPLETED_LOG"
    continue
  fi
done
