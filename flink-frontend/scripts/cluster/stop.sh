# Check if root, or running in no-exec mode
if [ "$EUID" -ne 0 ]
then echo "Please run as root"
  exit
fi

# -n (if any) should follow folder name (if any)
if [ "$1" = "-n" ]; then
  noexec=true
  elif [ ! -z "$1" ]; then
  folder="$1"
  answer=y
  if [ "$2" = "-n" ]; then
    noexec=true
    elif [ "$2" = "-a" ]; then
    answer=n
  fi
fi


CLUSTER="node10 node11 node12 node13 node14 node15 node20 node21 node22 node23 node182"
MASTER="node20"
FLINK_MASTER="node23"

if [ ! -z $noexec ]; then
  echo "[no-exec]"
fi
# Input previous folder name
if [ -z "$folder" ]; then
  ssh $MASTER "ls /home/auroflow/code/vector-search/syslogs"
  echo "Please input the previous syslogs folder name"
  read folder
else
  echo "Previous syslogs folder: $folder"
fi

# If -n argument is given, don't run flink
if [ ! -z $noexec ]; then
  if [ -z $answer ]; then
    echo "Do you want to keep the previous result? (y/n)"
    read answer
  fi
  if [ "$answer" != "${answer#[Nn]}" ] ;then
    echo "Drop the previous result"
  else
    echo "Keep the previous result"
  fi
  echo "done"
  exit
fi

# Ask if drop the previous result
if [ -z $answer ]; then
  echo "Do you want to keep the previous result? (y/n)"
  read answer
fi

for node in $CLUSTER; do
  echo "Killing alarm utility on $node"
  ssh $node 'pkill -f "bash /home/auroflow/code/vector-search/scripts/check.sh"'
done
echo "Killing flink memory monitor on $FLINK_MASTER"
ssh $FLINK_MASTER 'pkill -f "/usr/bin/python3 -u /home/auroflow/code/vector-search/scripts/monitor-flink-memory.py"'

# If Flink is running on master, stop
if ssh $FLINK_MASTER "/home/auroflow/java/amazon-corretto-8.362.08.1-linux-x64/bin/jps | grep StandaloneSessionClusterEntrypoint | grep -v grep"; then
  echo "Flink is running on master, stopping current job"
  ssh auroflow@$FLINK_MASTER -i /home/auroflow/.ssh/id_rsa "/home/auroflow/java/flink-1.18.0/bin/stop-cluster.sh"
fi

if [ "$answer" != "${answer#[Nn]}" ] ;then
  echo "Drop the previous result"
  for node in $CLUSTER; do
    echo "Removing syslogs and killing monitoring tools on $node"
    ssh $node "rm -rf /home/auroflow/code/vector-search/syslogs/$folder"
    ssh $node "pkill pidstat"
    ssh $node "pkill nethogs"
  done
else
  echo "Keep the previous result"
  for node in $CLUSTER; do
    echo "Saving Flink logs and killing monitoring tools on $node"
    ssh $node "cp -r /home/auroflow/java/flink-1.18.0/log /home/auroflow/code/vector-search/syslogs/$folder"
    # ssh $node "tree --du /data/auroflow/vector-search/rocksdb-stream/tmp > /home/auroflow/code/vector-search/syslogs/$folder/du.log"
    ssh $node "pkill pidstat"
    ssh $node "pkill nethogs"
  done
fi
echo "done"

