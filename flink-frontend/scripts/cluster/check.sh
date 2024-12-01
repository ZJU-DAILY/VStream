#!/bin/bash
source "$(dirname -- ${BASH_SOURCE})"/set_env.sh
# Regularly check if TaskManagerRunner is running
hostname=`hostname`
while true; do
  sleep 60

  # Check if TaskManagerRunner is running
  if /home/auroflow/java/amazon-corretto-8.362.08.1-linux-x64/bin/jps | grep -q TaskManagerRunner; then
    :
  else
     Send a message to http://warning.com via curl
    curl "$DINGTALK_NOTIFICATION_URL" \
      -H 'Content-Type: application/json' \
      -d '
      {
        "msgtype": "text",
        "text": {
          "content": "rocksdb-stream: TaskManagerRunner is not running on '$hostname'!"
        }
      }'
    exit
  fi
done
