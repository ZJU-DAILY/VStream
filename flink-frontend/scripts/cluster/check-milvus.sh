#!/bin/bash
source "$(dirname -- ${BASH_SOURCE})"/set_env.sh
hostname=`hostname`
while true; do
  sleep 60
  if ps aux | grep -q milvus; then
    :
  else
    curl "$DINGTALK_NOTIFICATION_URL" \
    -H 'Content-Type: application/json' \
    -d '
      {
        "msgtype": "text",
        "text": {
          "content": "rocksdb-stream: milvus is not running on '$hostname'!"
        }
    }'
    exit
  fi
done

