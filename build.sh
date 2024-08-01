mkdir -p build
cd build
cmake -DWITH_JNI:BOOL=ON -DWITH_TESTS:BOOL=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-DOfast -DNDEBUG" -DTRACER:BOOL=ON -DWITH_TOOLS:BOOL=OFF -DWITH_BENCHMARK_TOOLS:BOOL=OFF ..
make -j$(nproc)
ln -s librocksdbjni-shared.so java/librocksdbjni.so
# Copy rocksdbjni_classes.jar to $FLINK_HOME/lib
if [ -d "$FLINK_HOME/lib" ]; then
  cp java/rocksdbjni_classes.jar $FLINK_HOME/lib
else
  echo "FLINK_HOME not found. You should manually copy build/java/rocksdbjni_classes.jar to your flink lib directory."
fi
cd ../flink-frontend
mvn clean package -DskipTests
