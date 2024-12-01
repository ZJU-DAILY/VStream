#!/bin/bash
# Check if we are running in the VStream directory
if [ ! -f "workers" ]; then
  echo "Please run this script from the VStream directory"
  exit 1
fi

PROJECT_DIR=/home/auroflow/code/vector-search/VStream
BUILD_FLAGS=$1
SSH_PORT=4399

# The first node in the workers file is the master
master=$(head -n 1 workers)
# The rest are workers
workers=$(tail -n +2 workers)

# Copy the project to the master, excluding anything from the build directory
echo "Copying project to $master"
rsync -a --delete --exclude "/flink-frontend/nohup.out" --exclude ".idea" --exclude "/build/" --exclude "/build-ncompress/" --exclude "/cmake-build-*" --exclude "/examples/" --exclude "/flink-frontend/tmp" --exclude "/flink-frontend/params/completed" --exclude ".git" $PROJECT_DIR/ $master:$PROJECT_DIR

for worker in $workers; do
  echo "Copying project to $worker"
  # Copy the project to the worker, excluding anything from the build directory
  rsync -a --delete --exclude "/flink-frontend/nohup.out" --exclude ".idea" --exclude "/build/" --exclude "/build-ncompress/" --exclude "/cmake-build-*" --exclude "/examples/" --exclude "/flink-frontend/tmp" --exclude "/flink-frontend/params/completed" --exclude ".git" $PROJECT_DIR/ $worker:$PROJECT_DIR
done

if [ "$1" != "scripts" ]; then
  echo "Building..."
  ssh -p $SSH_PORT $master "cd $PROJECT_DIR && ./build.sh $BUILD_FLAGS" &
  for worker in $workers; do
    ssh -p $SSH_PORT $worker "cd $PROJECT_DIR && ./build.sh $BUILD_FLAGS" &
  done
  wait
fi
echo "all done"
