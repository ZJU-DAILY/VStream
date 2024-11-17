# Check if root
while getopts f:n:a: flag
do
  case "${flag}" in
    a) answer=${OPTARG};;
    n) noexec=${OPTARG};;
    f) folder=${OPTARG};;
    *) echo "Invalid flag: ${flag}";;
  esac
done

SCRIPT_DIR=$(dirname -- "${BASH_SOURCE}")
CLUSTER="node10 node11 node12 node13 node14 node15 node21 node22 node23 node182"
FLINK_MASTER="node11"
QDRANT_DATA_PATH="/home/auroflow/storage/qdrant"
FLINK_MASTER_PORT="4978"
RUN_HOST="node11"
MONITORED="$CLUSTER"
SSH_CONFIG="-p 4399"
SYSLOG_DIR="/home/auroflow/storage/syslogs"
FLINK_FRONTEND_DIR="/home/auroflow/code/vector-search/VStream/flink-frontend"

# Input previous folder name
if [ -z "$folder" ]; then
  ssh $SSH_CONFIG $RUN_HOST "ls $SYSLOG_DIR"
  echo "Please input the previous syslogs folder name"
  read folder
else
  echo "Previous syslogs folder: $folder"
fi


# If -n argument is given, don't run flink
if [ "$noexec" == "true" ]; then
  if [ -z "$answer" ]; then
    echo "Do you want to keep the previous result? (y/n)"
    read -r answer
  fi
  if [ "$answer" != "${answer#[Nn]}" ]; then
    echo "Drop the previous result"
  else
    echo "Keep the previous result"
  fi
  echo "[no-exec] done"
  exit
fi

# Ask if drop the previous result
if [ -z "$answer" ]; then
  echo "Do you want to keep the previous result? (y/n)"
  read -r answer
fi

if [ "$answer" == "${answer#[Nn]}" ]; then
  echo "Saving du log..."
  for node in $MONITORED; do
    ssh $SSH_CONFIG root@$node "tree --du $QDRANT_DATA_PATH > $SYSLOG_DIR/$folder/du.log"
  done
fi

# If Flink is running on master, stop
if ssh $SSH_CONFIG $FLINK_MASTER "jps | grep StandaloneSessionClusterEntrypoint | grep -v grep"; then
  echo "Flink is running on master, stopping current job"
  ssh $SSH_CONFIG $FLINK_MASTER "\$FLINK_HOME/bin/stop-cluster.sh"
fi


if [ "$answer" != "${answer#[Nn]}" ] ;then
  echo "Drop the previous result"
  echo "Dropping..."
  for node in $MONITORED; do
    ssh $SSH_CONFIG $node "rm -rf $SYSLOG_DIR/$folder"
    ssh $SSH_CONFIG root@$node "pkill pidstat"
    ssh $SSH_CONFIG root@$node "pkill nethogs"
  done
else
  echo "Keep the previous result"
  echo "Saving..."
  for node in $CLUSTER; do
    ssh $SSH_CONFIG $node "cp -r \$FLINK_HOME/log $SYSLOG_DIR/$folder"
  done
  for node in $MONITORED; do
    ssh $SSH_CONFIG root@$node "pkill pidstat"
    ssh $SSH_CONFIG root@$node "pkill nethogs"
  done
fi
echo "done"

