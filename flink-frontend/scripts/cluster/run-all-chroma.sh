#!/bin/bash
SCRIPT_DIR=$(dirname -- "${BASH_SOURCE}")
source "$SCRIPT_DIR/set_env.sh"
FLINK_FRONTEND_DIR=/home/auroflow/code/vector-search/VStream/flink-frontend
COMPLETED_LOG="$FLINK_FRONTEND_DIR/params/completed"

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
  # if filename contains 'chroma', invoke start-chroma.sh
  if [[ $file == *"chroma"* ]]; then
    echo "Running chroma job: $file"

    # Restart milvus cluster
    bash -c "cd $SCRIPT_DIR/../chroma && ./stop-chroma-cluster.sh prod"
    echo "Chroma cluster stopped."
    bash -c "cd $SCRIPT_DIR/../chroma && ./start-chroma-cluster.sh prod"
    echo "Chroma cluster started."

    "$SCRIPT_DIR/start-chroma.sh" -p "$file" -f true
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
    "$SCRIPT_DIR/stop-chroma.sh" -f "$syslog_dirname" -a y
    rm -f ./.syslog_dir
    if [ $status -ne 0 ]; then
      echo "fail $base" >> "$COMPLETED_LOG"
    else
      echo "done $base" >> "$COMPLETED_LOG"
    fi
  else
    echo "Skip non-chroma job $base."
    continue
  fi
done
