# check if root
source "$(dirname -- ${BASH_SOURCE})"/set_env.sh
if [ "$EUID" -ne 0 ]
then echo "Please run as root"
  exit
fi

MILVUS_ROOT="node182"

# Backup params.yaml
cp /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml ./params/params.yaml.bak

# Find all params.yaml files in the params directory
for file in $(find ./params -name "*.yaml" | sort -n); do
  
  DATE=$(date | tr -d \\n)
  
  # Copy params.yaml to the project directory
  cp $file /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml
  
  # Run the job, and save the syslogs folder name (last 9 characters)
  # if filename contains 'hnswlib', invoke start-hnswlib.sh
  if [[ $file == *"milvus"* ]]; then
    echo "Running milvus job..."

    # Restart milvus cluster
    ssh $MILVUS_ROOT "cd /home/auroflow/milvus/milvus-cluster-deploy-script/ && ./stop-cluster.sh"
    echo "Milvus cluster stopped."
    ssh $MILVUS_ROOT "rm -rf /home/auroflow/milvus/milvus-cluster-deploy-script/volumes"
    echo "Milvus data removed."
    ssh $MILVUS_ROOT "cd /home/auroflow/milvus/milvus-cluster-deploy-script/ && ./run-cluster.sh"
    echo "Milvus cluster started."

    folder=$(./start-milvus.sh -f | grep "Syslogs folder name" | awk 'NF{ print $NF }')
    bn=$(basename $folder)
    
    # Get the exit code of check-flink-task-manager.py
    /usr/bin/python3 check-flink-task-manager.py
    if [ $? -ne 0 ]; then
      echo "node10 should not be the partitioner. Terminating..."
      ./stop-milvus.sh $bn -a
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
      ./stop-milvus.sh $bn
      if [ $status -ne 0 ]; then
        mv $file "$file.failed"
      else
        mv $file "$file.done"
      fi
    fi
  else
    echo "Skip non-milvus job."
    mv $file "$file.skipped"
    continue
  fi
done

cp ./params/params.yaml.bak /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml

# if there is still '.yaml' file, rerun
if [ -f ./params/*.yaml ]; then
  echo "Rerunning..."
  ./run-all.sh
fi
