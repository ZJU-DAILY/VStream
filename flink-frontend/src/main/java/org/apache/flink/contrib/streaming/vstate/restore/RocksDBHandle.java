/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.vstate.restore;

import static org.apache.flink.contrib.streaming.vstate.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.contrib.streaming.vstate.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.vstate.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.vstate.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.vstate.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.vstate.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.rocksdb.*;

/**
 * Utility for creating a RocksDB instance either from scratch or from restored local state. This
 * will also register {@link RocksDbKvStateInfo} when using {@link #openDB(List, List, Path)}.
 */
@Slf4j
class RocksDBHandle implements AutoCloseable {

    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;
    private final Function<String, VectorColumnFamilyOptions> vectorCFOptionsFactory;
    private final Function<String, ColumnFamilyOptions> vectorVersionCFOptionsFactory;
    private final DBOptions dbOptions;
    private final Map<String, RocksDbKvStateInfo> kvStateInformation;
    private final String dbPath;
    private List<ColumnFamilyHandle> columnFamilyHandles;
    private List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    private List<VectorColumnFamilyHandle> vectorCFHandles;
    private List<VectorCFDescriptor> vectorCFDescriptors;
    private final RocksDBNativeMetricOptions nativeMetricOptions;
    private final MetricGroup metricGroup;
    // Current places to set compact filter into column family options:
    // - Incremental restore
    //   - restore with rescaling
    //     - init from a certain sst: #createAndRegisterColumnFamilyDescriptors when prepare files,
    // before db open
    //     - data ingestion after db open: #getOrRegisterStateColumnFamilyHandle before creating
    // column family
    //   - restore without rescaling: #createAndRegisterColumnFamilyDescriptors when prepare files,
    // before db open
    // - Full restore
    //   - data ingestion after db open: #getOrRegisterStateColumnFamilyHandle before creating
    // column family
    private final RocksDbTtlCompactFiltersManager ttlCompactFiltersManager;

    private RocksDB db;
    private ColumnFamilyHandle defaultColumnFamilyHandle;
    private RocksDBNativeMetricMonitor nativeMetricMonitor;
    private final Long writeBufferManagerCapacity;

    protected RocksDBHandle(
            Map<String, RocksDbKvStateInfo> kvStateInformation,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            Function<String, VectorColumnFamilyOptions> vectorCFOptionsFactory,
            Function<String, ColumnFamilyOptions> vectorVersionCFOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            Long writeBufferManagerCapacity) {
        this.kvStateInformation = kvStateInformation;
        this.dbPath = instanceRocksDBPath.getAbsolutePath();
        this.dbOptions = dbOptions;
        this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
        this.vectorCFOptionsFactory = vectorCFOptionsFactory;
        this.vectorVersionCFOptionsFactory = vectorVersionCFOptionsFactory;
        this.nativeMetricOptions = nativeMetricOptions;
        this.metricGroup = metricGroup;
        this.ttlCompactFiltersManager = ttlCompactFiltersManager;
        this.columnFamilyHandles = new ArrayList<>(1);
        this.columnFamilyDescriptors = Collections.emptyList();
        this.writeBufferManagerCapacity = writeBufferManagerCapacity;
    }

    void openDB() throws IOException {
        loadDb();
    }

    void openDB(
            @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            @Nonnull Path restoreSourcePath)
            throws IOException {
        this.columnFamilyDescriptors = columnFamilyDescriptors;
        this.columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);
        restoreInstanceDirectoryFromPath(restoreSourcePath);
        loadDb();
        // Register CF handlers
        for (int i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            getOrRegisterStateColumnFamilyHandle(
                    columnFamilyHandles.get(i), stateMetaInfoSnapshots.get(i));
        }
    }

    void reopenDB(
            @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            @Nonnull List<VectorCFDescriptor> vectorCFDescriptors,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            @Nonnull Path restoreSourcePath)
            throws IOException {
        this.columnFamilyDescriptors = columnFamilyDescriptors;
        this.vectorCFDescriptors = vectorCFDescriptors;
        this.columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);
        this.vectorCFHandles = new ArrayList<>(vectorCFDescriptors.size() + 1);
        restoreInstanceDirectoryFromPath(restoreSourcePath);
        reloadDb();
        // Register CF handlers
        int index;
        index = 0;
        for (int i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            if (stateMetaInfoSnapshots.get(i).getName().startsWith("vector-")) {
                getOrRegisterVectorStateColumnFamilyHandle(
                        vectorCFHandles.get(index), stateMetaInfoSnapshots.get(i));
                index++;
            }
        }
        index = 0;
        for (int i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            if (!stateMetaInfoSnapshots.get(i).getName().startsWith("vector-")) {
                getOrRegisterStateColumnFamilyHandle(
                        columnFamilyHandles.get(index), stateMetaInfoSnapshots.get(i));
                index++;
            }
        }
    }

    private void loadDb() throws IOException {
        db =
                RocksDBOperationUtils.openDB(
                        dbPath,
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        RocksDBOperationUtils.createColumnFamilyOptions(
                                columnFamilyOptionsFactory, "default"),
                        dbOptions);
        // remove the default column family which is located at the first index
        defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
        // init native metrics monitor if configured
        nativeMetricMonitor =
                nativeMetricOptions.isEnabled()
                        ? new RocksDBNativeMetricMonitor(
                                nativeMetricOptions, metricGroup, db, dbOptions.statistics())
                        : null;
    }

    private void reloadDb() throws IOException {
        db =
                RocksDBOperationUtils.openDB(
                        dbPath,
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        vectorCFDescriptors,
                        vectorCFHandles,
                        RocksDBOperationUtils.createColumnFamilyOptions(
                                columnFamilyOptionsFactory, "default"),
                        dbOptions);
        // remove the default column family which is located at the first index
        defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
        // init native metrics monitor if configured
        nativeMetricMonitor =
                nativeMetricOptions.isEnabled()
                        ? new RocksDBNativeMetricMonitor(
                                nativeMetricOptions, metricGroup, db, dbOptions.statistics())
                        : null;
    }

    RocksDbKvStateInfo getOrRegisterStateColumnFamilyHandle(
            ColumnFamilyHandle columnFamilyHandle, StateMetaInfoSnapshot stateMetaInfoSnapshot) {

        RocksDbKvStateInfo registeredStateMetaInfoEntry =
                kvStateInformation.get(stateMetaInfoSnapshot.getName());

        if (null == registeredStateMetaInfoEntry) {
            // create a meta info for the state on restore;
            // this allows us to retain the state in future snapshots even if it wasn't accessed
            RegisteredStateMetaInfoBase stateMetaInfo =
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
            if (columnFamilyHandle == null) {
                registeredStateMetaInfoEntry =
                        RocksDBOperationUtils.createStateInfo(
                                stateMetaInfo,
                                db,
                                columnFamilyOptionsFactory,
                                ttlCompactFiltersManager,
                                writeBufferManagerCapacity);
            } else {
                registeredStateMetaInfoEntry =
                        new RocksDbKvStateInfo(columnFamilyHandle, stateMetaInfo);
            }

            RocksDBOperationUtils.registerKvStateInformation(
                    kvStateInformation,
                    nativeMetricMonitor,
                    stateMetaInfoSnapshot.getName(),
                    registeredStateMetaInfoEntry);
        } else {
            // TODO with eager state registration in place, check here for serializer migration
            // strategies
        }

        return registeredStateMetaInfoEntry;
    }

    RocksDbKvStateInfo getOrRegisterVectorStateColumnFamilyHandle(
            VectorColumnFamilyHandle columnFamilyHandle,
            StateMetaInfoSnapshot stateMetaInfoSnapshot) {

        RocksDbKvStateInfo registeredStateMetaInfoEntry =
                kvStateInformation.get(stateMetaInfoSnapshot.getName());

        if (null == registeredStateMetaInfoEntry) {
            // create a meta info for the state on restore;
            // this allows us to retain the state in future snapshots even if it wasn't accessed
            RegisteredStateMetaInfoBase stateMetaInfo =
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
            if (columnFamilyHandle == null) {
                registeredStateMetaInfoEntry =
                        RocksDBOperationUtils.createStateInfo(
                                stateMetaInfo,
                                db,
                                columnFamilyOptionsFactory,
                                ttlCompactFiltersManager,
                                writeBufferManagerCapacity);
            } else {
                registeredStateMetaInfoEntry =
                        new RocksDbKvStateInfo(columnFamilyHandle, stateMetaInfo);
            }

            RocksDBOperationUtils.registerKvStateInformation(
                    kvStateInformation,
                    nativeMetricMonitor,
                    stateMetaInfoSnapshot.getName(),
                    registeredStateMetaInfoEntry);
        } else {
            // TODO with eager state registration in place, check here for serializer migration
            // strategies
        }

        return registeredStateMetaInfoEntry;
    }

    /**
     * This recreates the new working directory of the recovered RocksDB instance and links/copies
     * the contents from a local state.
     */
    private void restoreInstanceDirectoryFromPath(Path source) throws IOException {
        final Path instanceRocksDBDirectory = Paths.get(dbPath);
        final Path[] files = FileUtils.listDirectory(source);

        if (!new File(dbPath).mkdirs()) {
            String errMsg = "Could not create RocksDB data directory: " + dbPath;
            LOG.error(errMsg);
            throw new IOException(errMsg);
        }

        for (Path file : files) {
            final String fileName = file.getFileName().toString();
            final Path targetFile = instanceRocksDBDirectory.resolve(fileName);
            if (fileName.endsWith(SST_FILE_SUFFIX)) {
                try {
                    // hardlink'ing the immutable sst-files.
                    Files.createLink(targetFile, file);
                    continue;
                } catch (IOException ioe) {
                    final String logMessage =
                            String.format(
                                    "Could not hard link sst file %s. Trying to copy it over. This might "
                                            + "increase the recovery time. In order to avoid this, configure "
                                            + "RocksDB's working directory and the local state directory to be on the same volume.",
                                    fileName);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(logMessage, ioe);
                    } else {
                        LOG.info(logMessage);
                    }
                }
            }

            // true copy for all other files and files that could not be hard linked.
            Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public RocksDB getDb() {
        return db;
    }

    public RocksDBNativeMetricMonitor getNativeMetricMonitor() {
        return nativeMetricMonitor;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    public List<ColumnFamilyHandle> getColumnFamilyHandles() {
        return columnFamilyHandles;
    }

    public RocksDbTtlCompactFiltersManager getTtlCompactFiltersManager() {
        return ttlCompactFiltersManager;
    }

    public Long getWriteBufferManagerCapacity() {
        return writeBufferManagerCapacity;
    }

    public Function<String, ColumnFamilyOptions> getColumnFamilyOptionsFactory() {
        return columnFamilyOptionsFactory;
    }

    public Function<String, VectorColumnFamilyOptions> getVectorCFOptionsFactory() {
        return vectorCFOptionsFactory;
    }

    public Function<String, ColumnFamilyOptions> getVectorVersionCFOptionsFactory() {
        return vectorVersionCFOptionsFactory;
    }

    public DBOptions getDbOptions() {
        return dbOptions;
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeQuietly(nativeMetricMonitor);
        IOUtils.closeQuietly(db);
        // Making sure the already created column family options will be closed
        columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));
    }
}
