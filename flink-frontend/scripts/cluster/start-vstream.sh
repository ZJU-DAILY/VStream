# check if root
if [ "$EUID" -ne 0 ]
then echo "Please run as root"
  exit
fi

echo "Running rocksdb backend"

CLUSTER="node10 node11 node12 node13 node14 node15 node20 node21 node22 node23 node182"
MASTER="node20"
FLINK_MASTER="node23"

if [ "$1" = "-n" ]; then
  noexec=true
fi
if [ "$2" = "-n" ]; then
  noexec=true
fi
if [ "$1" = "-f" ]; then
  force=true
fi
if [ "$2" = "-f" ]; then
  force=true
fi

# On node20
# Show config
ssh $MASTER "cat /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml"
if [ ! -z $noexec ]; then
  echo "[no-exec]"
fi
# confirm correct
if [ -z $force ]; then
  echo "Are the params correct?"
  read iscorrect
  if [ "$iscorrect" != "y" ]
  then echo "vi /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml"
    exit
  fi
fi

# Get current time
DATE=$(date +"%m%d-%H%M")
folder="/home/auroflow/code/vector-search/syslogs/$DATE-rocksdb"
echo "Syslogs folder name: $folder"

# If -n argument is given, don't run flink
if [ ! -z $noexec ]; then
  echo "done"
  exit
fi

# If Flink is running on master, exit
if ssh $FLINK_MASTER "/home/auroflow/java/amazon-corretto-8.362.08.1-linux-x64/bin/jps | grep StandaloneSessionClusterEntrypoint | grep -v grep"; then
  echo "Flink is running on master, please stop Flink first"
  exit
fi

# Copy current config to master's syslogs folder
ssh $MASTER "mkdir -p $folder"
ssh $MASTER "cp /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml $folder"

# Remove flink logs on all nodes
for node in $CLUSTER; do
  echo "Removing old Flink logs on $node"
  ssh $node "rm -rf /home/auroflow/java/flink-1.18.0/log/*"
done

# clear previous RocksDB storage
for node in $CLUSTER; do
  echo "Clearing RocksDB storage on $node"
  ssh $node "rm -rf /data/auroflow/vector-search/rocksdb-stream/tmp/*/job_*"
done

python3 calculate-parallelism.py /home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml

# Restart flink cluster
ssh auroflow@$FLINK_MASTER -i /home/auroflow/.ssh/id_rsa "cd /home/auroflow/java/flink-1.18.0/; ./bin/start-cluster.sh"
# On every machine
for node in $CLUSTER; do
  echo "Starting monitoring tools on $node"
  ssh $node "mkdir -p $folder"
  ssh $node 'bash -c "ps aux >> '$folder'/ps.log"'
  ssh $node 'nohup bash -c "/usr/local/bin/pidstat -C java 10 -urd -p ALL > '$folder'/pidstat.log 2>&1 &" </dev/null >/dev/null 2>/dev/null'
  ssh $node 'nohup bash -c "/usr/sbin/nethogs -t -d 10 > '$folder'/nethogs.log 2>&1 &" </dev/null >/dev/null 2>/dev/null'
  ssh $node 'nohup bash -c "bash /home/auroflow/code/vector-search/scripts/check.sh >/dev/null 2>&1 &" </dev/null >/dev/null 2>/dev/null'
done

# On node23
ssh $FLINK_MASTER 'nohup bash -c "/usr/bin/python3 -u /home/auroflow/code/vector-search/scripts/monitor-flink-memory.py > '$folder'/monitor.log 2>&1 &" </dev/null >/dev/null 2>/dev/null'
# remove nohup
ssh $MASTER "rm -f /home/auroflow/code/vector-search/rocksdb-stream/nohup.out"
# Run flink job
echo "Starting Flink job..."
ssh auroflow@$MASTER -i /home/auroflow/.ssh/id_rsa 'flink run -c cn.edu.zju.daily.RocksDBStreamSearchJob --jobmanager=10.214.151.23:8081 /home/auroflow/code/vector-search/rocksdb-stream/target/rocksdb-stream-1.0-SNAPSHOT.jar'
echo "done"
