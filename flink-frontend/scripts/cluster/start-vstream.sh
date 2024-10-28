#!/bin/bash

while getopts p:n:f: flag
do
  case "${flag}" in
    p) param_file=${OPTARG};;
    n) noexec=${OPTARG};;
    f) force=${OPTARG};;
    *) echo "Invalid flag: ${flag}";;
  esac
done

CLUSTER="node10 node11 node12 node13 node14 node15 node21 node22 node23 node182"
FLINK_MASTER="node11"
FLINK_MASTER_PORT="4978"
RUN_HOST="node182"
MONITORED="$CLUSTER"
SSH_CONFIG="-p 4399"
SYSLOG_DIR="/home/auroflow/storage/syslogs"
STORAGE_DIR="/home/auroflow/storage/rocksdb"
FLINK_FRONTEND_DIR="/home/auroflow/code/vector-search/VStream/flink-frontend"
FLINK_FRONTEND_JAR="/home/auroflow/code/vector-search/VStream/build/flink-frontend/vstream-1.1.jar"

# Display config
echo "About to execute:"
ssh $SSH_CONFIG $RUN_HOST "cat $FLINK_FRONTEND_DIR/src/main/resources/params.yaml"

# confirm correct
if [ "$force" != "true" ]; then
  echo "Are the params correct?"
  read -r iscorrect
  if [ "$iscorrect" != "y" ]; then
    echo "vi $FLINK_FRONTEND_DIR/src/main/resources/params.yaml"
    exit
  fi
fi

# Get current time
DATE=$(date +"%m%d-%H%M")
syslog_dir="$SYSLOG_DIR/$DATE-vstream"
echo "Syslogs folder name: $syslog_dir"

if [ "$noexec" == "true" ]; then
  echo "[no-exec] job started"
  exit
fi


# If Flink is running on master, exit
if ssh $SSH_CONFIG $FLINK_MASTER "jps | grep StandaloneSessionClusterEntrypoint | grep -v grep"; then
  echo "Flink is running on $FLINK_MASTER, please stop Flink first"
  exit
fi

# sync params.yaml to RUN_HOST
rsync -a -e "ssh $SSH_CONFIG" "$param_file" $RUN_HOST:$FLINK_FRONTEND_DIR/src/main/resources/params.yaml

# Copy current config to master's syslogs folder
ssh $SSH_CONFIG $RUN_HOST "mkdir -p $syslog_dir"
ssh $SSH_CONFIG $RUN_HOST "cp $FLINK_FRONTEND_DIR/src/main/resources/params.yaml $syslog_dir"

# Remove flink logs on all nodes
for node in $CLUSTER; do
  echo "Removing old Flink logs on $node"
  ssh $SSH_CONFIG $node "rm -rf \$FLINK_HOME/log/*"
done

python3 "$(dirname -- ${BASH_SOURCE})"/adjust_parallelism.py "$param_file"

# clear previous RocksDB storage
for node in $CLUSTER; do
  echo "Clearing RocksDB storage on $node"
  ssh $SSH_CONFIG $node "mkdir -p $STORAGE_DIR"
  ssh $SSH_CONFIG $node "rm -rf $STORAGE_DIR/*/job_*"
done

# Restart flink cluster
ssh $SSH_CONFIG $FLINK_MASTER "\$FLINK_HOME/bin/start-cluster.sh"
# On every machine
echo "Starting monitoring tools..."
# On every machine
for node in $MONITORED; do
  ssh $SSH_CONFIG $node "mkdir -p $syslog_dir"
  ssh $SSH_CONFIG root@$node 'bash -c "ps aux >> '$syslog_dir'/ps.log"'
  ssh $SSH_CONFIG root@$node 'nohup bash -c "/usr/local/bin/pidstat 2 -urd -p $(pgrep -f "flink" | tr "\n" ",") > '$syslog_dir'/pidstat.log 2>&1 &" </dev/null >/dev/null 2>/dev/null'
  ssh $SSH_CONFIG root@$node 'nohup bash -c "/usr/sbin/nethogs -t -d 2 > '$syslog_dir'/nethogs.log 2>&1 &" </dev/null >/dev/null 2>/dev/null'
done

# On node23
ssh $SSH_CONFIG $FLINK_MASTER 'nohup bash -c "/usr/bin/python3 -u '$FLINK_FRONTEND_DIR'/scripts/cluster/monitor-flink-memory.py > '$syslog_dir'/monitor.log 2>&1 &" </dev/null >/dev/null 2>/dev/null'
# remove nohup
ssh $SSH_CONFIG $RUN_HOST "rm $FLINK_FRONTEND_DIR/nohup.out &> /dev/null"
# Run flink job
echo "Starting Flink job..."
ssh $SSH_CONFIG $RUN_HOST 'nohup flink run -c cn.edu.zju.daily.VStreamSearchJob --jobmanager='$FLINK_MASTER':'$FLINK_MASTER_PORT' '$FLINK_FRONTEND_JAR' '$FLINK_FRONTEND_DIR'/src/main/resources/params.yaml > '$FLINK_FRONTEND_DIR'/nohup.out &'
echo "$syslog_dir" > ./.syslog_dir

echo "job started"
