# Check if root, or running in no-exec mode
if [ "$EUID" -ne 0 ]
then echo "Please run as root"
  exit
fi

if [ "$1" = "-n" ]; then
  noexec=true
  elif [ ! -z "$1" ]; then
  folder="$1"
  answer=y
  if [ "$2" = "-n" ]; then
    noexec=true
  fi
fi


CLUSTER="node10 node11 node12 node13 node14 node15 node182 node21 node22 node23 node25"
MASTER="noe11"
# Input previous folder name
ssh $MASTER "ls /home/auroflow/code/vector-search/syslogs"
if [ ! -z $noexec ]; then
  echo "[no-exec]"
fi
if [ -z "$folder" ]; then
  echo "Please input the previous syslogs folder name"
  read folder
else
  echo "Previous syslogs folder: $folder"
fi

for node in $CLUSTER; do
  echo "Saving Flink logs on $node"
  ssh $node "cp -r /home/auroflow/java/flink-1.18.0/log /home/auroflow/code/vector-search/syslogs/$folder"
done
echo "done"

