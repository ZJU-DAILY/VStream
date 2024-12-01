#!/bin/bash

if [ "$1" != "inc" ]; then
  rm -rf build 2> /dev/null
fi

mkdir -p build
cd build || exit 1

# Use dev toolset for CentOS 7
if [ -f /etc/os-release ]; then
  source /etc/os-release
  OS=$NAME
  VER=$VERSION_ID
  if [ "$OS" == "CentOS Linux" ] && [ "$VER" == "7" ]; then
    source /opt/rh/devtoolset-11/enable
  fi
fi

echo "Building on $(hostname)"

cmake -DWITH_JNI:BOOL=ON -DWITH_TESTS:BOOL=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fpermissive" -DTRACER:BOOL=ON -DWITH_TOOLS:BOOL=OFF -DWITH_BENCHMARK_TOOLS:BOOL=OFF ..
make -j"$(nproc)"

# success?
if [ "$?" -ne 0 ]; then
  echo "Build vstream failed."
  exit 1
fi

ln -sf librocksdbjni-shared.so java/librocksdbjni.so
# Copy rocksdbjni_classes.jar to $FLINK_HOME/lib
if [ -d "$FLINK_HOME/lib" ]; then
  cp java/rocksdbjni_classes.jar $FLINK_HOME/lib
else
  echo "FLINK_HOME not found. You should manually copy build/java/rocksdbjni_classes.jar to your Flink lib directory."
fi

cd ../flink-frontend || exit 1
mvn clean package -DskipTests -Dbuild.keyword=build

# success?
if [ "$?" -ne 0 ]; then
  echo "Build flink-frontend failed."
  exit 1
fi

echo "Build complete. Make sure to do the following on every machine in your Flink cluster:
  - Put build/java/librocksdbjni-shared.so in \$LD_LIBRARY_PATH and rename it to librocksdbjni.so.
  - Copy build/java/rocksdbjni_classes.jar to \$FLINK_HOME/lib directory.
If you are running deploy.sh, these steps will be done next."
