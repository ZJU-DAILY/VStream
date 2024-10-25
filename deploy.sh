#!/bin/bash
# Check if we are running in the VStream directory
if [ ! -f "workers" ]; then
  echo "Please run this script from the VStream directory"
  exit 1
fi

REMOTE_PROJECT_DIR=/home/auroflow/code/vector-search/VStream
BUILD_FLAGS=$1
SSH_PORT=4399

# The first node in the workers file is the master
master=$(head -n 1 workers)
# The rest are workers
workers=$(tail -n +2 workers)

# Copy the project to the master, excluding anything from the build directory
rsync -avz --delete --exclude "/flink-frontend/nohup.out" --exclude "/build/" --exclude "/cmake-build-*" --exclude "/examples/" --exclude "/flink-frontend/tmp" --exclude "/flink-frontend/params/completed" --exclude ".git" . $master:$REMOTE_PROJECT_DIR

if [ "$BUILD_FLAGS" == "scripts" ]; then
  echo "Scripts copied to master. Done."
  exit 0
fi

ssh -p $SSH_PORT $master "cd $REMOTE_PROJECT_DIR && ./build.sh $BUILD_FLAGS"

# success?
if [ $? -ne 0 ]; then
  echo "Remote build failed."
  exit 1
fi

for worker in $workers; do
  # Copy the build directory to the worker
  ssh $master -p $SSH_PORT "rsync -azv -e \"ssh -p $SSH_PORT\" --exclude flink-frontend/ $REMOTE_PROJECT_DIR/build '$worker':$REMOTE_PROJECT_DIR"
  # Copy rocksdbjni_classes.jar to $FLINK_HOME/lib
  ssh $worker -p $SSH_PORT "\
    if [ -d \"\$FLINK_HOME/lib\" ]; then \
      cp $REMOTE_PROJECT_DIR/build/java/rocksdbjni_classes.jar \$FLINK_HOME/lib; \
    else \
      echo \"\$(hostname): FLINK_HOME not found. You should manually copy $REMOTE_PROJECT_DIR/build/java/rocksdbjni_classes.jar to your Flink lib directory.\";\
    fi"
done
echo "all done"
