#!/bin/bash

if [ "$1" != "inc" ]; then
  rm -rf build 2> /dev/null
fi

mkdir -p build
cd build

# Comment out the next line if you are not using dev toolset
source /opt/rh/devtoolset-11/enable

cmake -DWITH_JNI:BOOL=ON -DWITH_TESTS:BOOL=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fpermissive" -DTRACER:BOOL=ON -DWITH_TOOLS:BOOL=OFF -DWITH_BENCHMARK_TOOLS:BOOL=OFF ..
make -j$(nproc)

# success?
if [ $? -ne 0 ]; then
  echo "Build vstream failed."
  exit 1
fi

ln -s librocksdbjni-shared.so java/librocksdbjni.so
# Copy rocksdbjni_classes.jar to $FLINK_HOME/lib
if [ -d "$FLINK_HOME/lib" ]; then
  cp java/rocksdbjni_classes.jar $FLINK_HOME/lib
else
  echo "FLINK_HOME not found. You should manually copy build/java/rocksdbjni_classes.jar to your Flink lib directory."
fi
cd ../flink-frontend
mvn clean package -DskipTests

# success?
if [ $? -ne 0 ]; then
  echo "Build flink-frontend failed."
  exit 1
fi

echo "Build complete. Make sure to do the following on every machine in your Flink cluster:
  - Put build/java/librocksdbjni-shared.so in \$LD_LIBRARY_PATH and rename it to librocksdbjni.so.
  - Copy build/java/rocksdbjni_classes.jar to \$FLINK_HOME/lib directory.
If you are running deploy.sh, these steps are done automatically."