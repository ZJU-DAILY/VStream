/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.vstate;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;
import static cn.edu.zju.daily.data.DataSerializer.*;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link MapState} implementation that stores state in RocksDB vector backend.
 *
 * @param <K>  The type of the key.
 * @param <N>  The type of the namespace.
 */
public class RocksDBMockVectorMapState<K, N> extends AbstractRocksDBState<K, N, Map<byte[], byte[]>>
    implements InternalMapState<K, N, byte[], byte[]> {

    @VisibleForTesting
    public class MockRocksDB {

        private final Map<Long, byte[]> map;
        private final int dim;
        private final int k;

        MockRocksDB() throws RocksDBException {
            this.map = new HashMap<>();
//            this.dim = (int) vectorColumnFamily.getDescriptor().getOptions().dim();
//            this.k = (int) searchOptions.k();
            this.dim = 128;
            this.k = 10;
        }

        float distance(float[] a, float[] b) {
            float sum = 0;
            for (int i = 0; i < a.length; i++) {
                sum += (a[i] - b[i]) * (a[i] - b[i]);
            }
            return (float) Math.sqrt(sum);
        }

        void put(
            VectorColumnFamilyHandle vectorColumnFamily, WriteOptions writeOptions, byte[] rawKeyBytes,
            byte[] rawValueBytes) throws RocksDBException {
            map.put(deserializeLong(rawKeyBytes), rawValueBytes);
        }

        byte[] vectorSearch(
            VectorColumnFamilyHandle vectorColumnFamily, VectorSearchOptions searchOptions, byte[] rawKeyBytes)
            throws RocksDBException {

            float[] vector = new float[dim];
            deserializeFloatArray(rawKeyBytes, vector);

            PriorityQueue<Tuple2<Long, Float>> queue = new PriorityQueue<>(k,
                Comparator.<Tuple2<Long, Float>, Float>comparing(tuple -> tuple.f1).reversed());

            for (Map.Entry<Long, byte[]> entry : map.entrySet()) {
                long id = entry.getKey();
                float[] value = new float[dim];
                deserializeFloatArray(entry.getValue(), value);

                float d = distance(vector, value);
                queue.add(new Tuple2<>(id, d));
                if (queue.size() > k) {
                    queue.poll();
                }
            }

            int numResults = queue.size();
            long[] ids = new long[numResults];
            float[] distances = new float[numResults];
            for (int i = numResults - 1; i >= 0; i--) {
                Tuple2<Long, Float> tuple = queue.poll();
                assert tuple != null;
                ids[i] = tuple.f0;
                distances[i] = tuple.f1;
            }

            // Result: [id0, dis0, id1, dis1, ...]
            byte[] results = new byte[numResults * 12];
            serializeResultArray(ids, distances, results);
            return results;
        }

        Set<Long> getKeys() {
            return map.keySet();
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMockVectorMapState.class);

    /**
     * Serializer for the keys and values.
     */
    private TypeSerializer<byte[]> userKeySerializer;

    private TypeSerializer<byte[]> userValueSerializer;

    private final VectorSearchOptions searchOptions;

    private final VectorColumnFamilyHandle vectorColumnFamily;

    private final MockRocksDB mdb;



    /**
     * Creates a new {@code RocksDBMockVectorMapState}.
     *
     * @param vectorColumnFamily        The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer     The serializer for the state.
     * @param defaultValue        The default value for the state.
     * @param backend             The backend for which this state is bind to.
     */
    private RocksDBMockVectorMapState(
        VectorColumnFamilyHandle vectorColumnFamily,
        TypeSerializer<N> namespaceSerializer,
        TypeSerializer<Map<byte[], byte[]>> valueSerializer,
        Map<byte[], byte[]> defaultValue,
        RocksDBKeyedStateBackend<K> backend) throws RocksDBException {

        super(null, namespaceSerializer, valueSerializer, defaultValue, backend);

        Preconditions.checkState(
            valueSerializer instanceof MapSerializer, "Unexpected serializer type.");

        this.vectorColumnFamily = vectorColumnFamily;
        MapSerializer<byte[], byte[]> castedMapSerializer = (MapSerializer<byte[], byte[]>) valueSerializer;
        this.userKeySerializer = castedMapSerializer.getKeySerializer();
        this.userValueSerializer = castedMapSerializer.getValueSerializer();
        this.searchOptions = backend.getVectorSearchOptions();
        this.mdb = new MockRocksDB();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<Map<byte[], byte[]>> getValueSerializer() {
        return valueSerializer;
    }

    // ------------------------------------------------------------------------
    //  MapState Implementation
    // ------------------------------------------------------------------------

    /**
     * Performs vector search.
     *
     * @param userKey The byte array of the vector to search for.
     * @return the result byte array.
     * @throws IOException
     * @throws RocksDBException
     */
    @Override
    public byte[] get(byte[] userKey) throws IOException, RocksDBException {

        if (userKey.length == 0) {
            // Dump
            List<Long> keys = new ArrayList<>(mdb.getKeys());
            byte[] results = new byte[keys.size() * 8];
            serializeLongList(keys, results);
            return results;
        }
        return mdb.vectorSearch(vectorColumnFamily, searchOptions, userKey);
    }

    /**
     * Performs vector insert.
     *
     * @param userKey   The byte array of the vector ID (long type) to insert.
     * @param userValue The byte array of the vector to insert.
     * @throws IOException
     * @throws RocksDBException
     */
    @Override
    public void put(byte[] userKey, byte[] userValue) throws IOException, RocksDBException {
        mdb.put(vectorColumnFamily, writeOptions, userKey, userValue);
    }

    @Override
    public void putAll(Map<byte[], byte[]> map) throws IOException, RocksDBException {
        if (map == null) {
            return;
        }

        try (RocksDBWriteBatchWrapper writeBatchWrapper =
                 new RocksDBWriteBatchWrapper(
                     backend.db, writeOptions, backend.getWriteBatchSize())) {
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                byte[] rawKeyBytes =
                    serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
                        entry.getKey(), userKeySerializer);
                byte[] rawValueBytes =
                    serializeValueNullSensitive(entry.getValue(), userValueSerializer);
                writeBatchWrapper.put(columnFamily, rawKeyBytes, rawValueBytes);
            }
        }
    }

    @Override
    public void remove(byte[] userKey) throws IOException, RocksDBException {
        byte[] rawKeyBytes =
            serializeCurrentKeyWithGroupAndNamespacePlusUserKey(userKey, userKeySerializer);

        backend.db.delete(columnFamily, writeOptions, rawKeyBytes);
    }

    @Override
    public boolean contains(byte[] userKey) throws IOException, RocksDBException {
        byte[] rawKeyBytes =
            serializeCurrentKeyWithGroupAndNamespacePlusUserKey(userKey, userKeySerializer);
        byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

        return (rawValueBytes != null);
    }

    @Override
    public Iterable<Map.Entry<byte[], byte[]>> entries() {
        return this::iterator;
    }

    @Override
    public Iterable<byte[]> keys() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        return () ->
            new RocksDBMapIterator<byte[]>(
                backend.db,
                prefixBytes,
                userKeySerializer,
                userValueSerializer,
                dataInputView) {
                @Nullable
                @Override
                public byte[] next() {
                    RocksDBMapEntry entry = nextEntry();
                    return (entry == null ? null : entry.getKey());
                }
            };
    }

    @Override
    public Iterable<byte[]> values() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        return () ->
            new RocksDBMapIterator<byte[]>(
                backend.db,
                prefixBytes,
                userKeySerializer,
                userValueSerializer,
                dataInputView) {
                @Override
                public byte[] next() {
                    RocksDBMapEntry entry = nextEntry();
                    return (entry == null ? null : entry.getValue());
                }
            };
    }

    @Override
    public void migrateSerializedValue(
        DataInputDeserializer serializedOldValueInput,
        DataOutputSerializer serializedMigratedValueOutput,
        TypeSerializer<Map<byte[], byte[]>> priorSerializer,
        TypeSerializer<Map<byte[], byte[]>> newSerializer)
        throws StateMigrationException {

        checkArgument(priorSerializer instanceof MapSerializer);
        checkArgument(newSerializer instanceof MapSerializer);

        TypeSerializer<byte[]> priorMapValueSerializer =
            ((MapSerializer<byte[], byte[]>) priorSerializer).getValueSerializer();
        TypeSerializer<byte[]> newMapValueSerializer =
            ((MapSerializer<byte[], byte[]>) newSerializer).getValueSerializer();

        try {
            boolean isNull = serializedOldValueInput.readBoolean();
            byte[] mapUserValue = null;
            if (!isNull) {
                mapUserValue = priorMapValueSerializer.deserialize(serializedOldValueInput);
            }
            serializedMigratedValueOutput.writeBoolean(mapUserValue == null);
            newMapValueSerializer.serialize(mapUserValue, serializedMigratedValueOutput);
        } catch (Exception e) {
            throw new StateMigrationException(
                "Error while trying to migrate RocksDB map state.", e);
        }
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        return new RocksDBMapIterator<Map.Entry<byte[], byte[]>>(
            backend.db, prefixBytes, userKeySerializer, userValueSerializer, dataInputView) {
            @Override
            public Map.Entry<byte[], byte[]> next() {
                return nextEntry();
            }
        };
    }

    @Override
    public boolean isEmpty() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        try (RocksIteratorWrapper iterator =
                 RocksDBOperationUtils.getRocksIterator(
                     backend.db, columnFamily, backend.getReadOptions())) {

            iterator.seek(prefixBytes);

            return !iterator.isValid() || !startWithKeyPrefix(prefixBytes, iterator.key());
        }
    }

    @Override
    public void clear() {
        try (RocksIteratorWrapper iterator =
                 RocksDBOperationUtils.getRocksIterator(
                     backend.db, columnFamily, backend.getReadOptions());
             RocksDBWriteBatchWrapper rocksDBWriteBatchWrapper =
                 new RocksDBWriteBatchWrapper(
                     backend.db,
                     backend.getWriteOptions(),
                     backend.getWriteBatchSize())) {

            final byte[] keyPrefixBytes = serializeCurrentKeyWithGroupAndNamespace();
            iterator.seek(keyPrefixBytes);

            while (iterator.isValid()) {
                byte[] keyBytes = iterator.key();
                if (startWithKeyPrefix(keyPrefixBytes, keyBytes)) {
                    rocksDBWriteBatchWrapper.remove(columnFamily, keyBytes);
                } else {
                    break;
                }
                iterator.next();
            }
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException("Error while cleaning the state in RocksDB.", e);
        }
    }

    @Override
    protected RocksDBMockVectorMapState<K, N> setValueSerializer(
        TypeSerializer<Map<byte[], byte[]>> valueSerializer) {
        super.setValueSerializer(valueSerializer);
        MapSerializer<byte[], byte[]> castedMapSerializer = (MapSerializer<byte[], byte[]>) valueSerializer;
        this.userKeySerializer = castedMapSerializer.getKeySerializer();
        this.userValueSerializer = castedMapSerializer.getValueSerializer();
        return this;
    }

    @Override
    public byte[] getSerializedValue(
        final byte[] serializedKeyAndNamespace,
        final TypeSerializer<K> safeKeySerializer,
        final TypeSerializer<N> safeNamespaceSerializer,
        final TypeSerializer<Map<byte[], byte[]>> safeValueSerializer)
        throws Exception {

        Preconditions.checkNotNull(serializedKeyAndNamespace);
        Preconditions.checkNotNull(safeKeySerializer);
        Preconditions.checkNotNull(safeNamespaceSerializer);
        Preconditions.checkNotNull(safeValueSerializer);

        // TODO make KvStateSerializer key-group aware to save this round trip and key-group
        // computation
        Tuple2<K, N> keyAndNamespace =
            KvStateSerializer.deserializeKeyAndNamespace(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        int keyGroup =
            KeyGroupRangeAssignment.assignToKeyGroup(
                keyAndNamespace.f0, backend.getNumberOfKeyGroups());

        SerializedCompositeKeyBuilder<K> keyBuilder =
            new SerializedCompositeKeyBuilder<>(
                safeKeySerializer, backend.getKeyGroupPrefixBytes(), 32);

        keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);

        final byte[] keyPrefixBytes =
            keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);

        final MapSerializer<byte[], byte[]> serializer = (MapSerializer<byte[], byte[]>) safeValueSerializer;

        final TypeSerializer<byte[]> dupUserKeySerializer = serializer.getKeySerializer();
        final TypeSerializer<byte[]> dupUserValueSerializer = serializer.getValueSerializer();
        final DataInputDeserializer inputView = new DataInputDeserializer();

        final Iterator<Map.Entry<byte[], byte[]>> iterator =
            new RocksDBMapIterator<Map.Entry<byte[], byte[]>>(
                backend.db,
                keyPrefixBytes,
                dupUserKeySerializer,
                dupUserValueSerializer,
                inputView) {

                @Override
                public Map.Entry<byte[], byte[]> next() {
                    return nextEntry();
                }
            };

        // Return null to make the behavior consistent with other backends
        if (!iterator.hasNext()) {
            return null;
        }

        return KvStateSerializer.serializeMap(
            () -> iterator, dupUserKeySerializer, dupUserValueSerializer);
    }

    // ------------------------------------------------------------------------
    //  Serialization Methods
    // ------------------------------------------------------------------------


    private static byte[] deserializeUserKey(
        DataInputDeserializer dataInputView,
        int userKeyOffset,
        byte[] rawKeyBytes,
        TypeSerializer<byte[]> keySerializer)
        throws IOException {
        dataInputView.setBuffer(rawKeyBytes, userKeyOffset, rawKeyBytes.length - userKeyOffset);
        return keySerializer.deserialize(dataInputView);
    }

    private static byte[] deserializeUserValue(
        DataInputDeserializer dataInputView,
        byte[] rawValueBytes,
        TypeSerializer<byte[]> valueSerializer)
        throws IOException {

        dataInputView.setBuffer(rawValueBytes);

        boolean isNull = dataInputView.readBoolean();

        return isNull ? null : valueSerializer.deserialize(dataInputView);
    }

    private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
        if (rawKeyBytes.length < keyPrefixBytes.length) {
            return false;
        }

        for (int i = keyPrefixBytes.length; --i >= backend.getKeyGroupPrefixBytes(); ) {
            if (rawKeyBytes[i] != keyPrefixBytes[i]) {
                return false;
            }
        }

        return true;
    }

    // ------------------------------------------------------------------------
    //  Internal Classes
    // ------------------------------------------------------------------------

    /**
     * A map entry in RocksDBMockVectorMapState.
     */
    private class RocksDBMapEntry implements Map.Entry<byte[], byte[]> {
        private final RocksDB db;

        /**
         * The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB with the
         * format #KeyGroup#Key#Namespace#UserKey.
         */
        private final byte[] rawKeyBytes;

        /**
         * The raw bytes of the value stored in RocksDB.
         */
        private byte[] rawValueBytes;

        /**
         * True if the entry has been deleted.
         */
        private boolean deleted;

        /**
         * The user key and value. The deserialization is performed lazily, i.e. the key and the
         * value is deserialized only when they are accessed.
         */
        private byte[] userKey;

        private byte[] userValue;

        /**
         * The offset of User Key offset in raw key bytes.
         */
        private final int userKeyOffset;

        private final TypeSerializer<byte[]> keySerializer;

        private final TypeSerializer<byte[]> valueSerializer;

        private final DataInputDeserializer dataInputView;

        RocksDBMapEntry(
            @Nonnull final RocksDB db,
            @Nonnegative final int userKeyOffset,
            @Nonnull final byte[] rawKeyBytes,
            @Nonnull final byte[] rawValueBytes,
            @Nonnull final TypeSerializer<byte[]> keySerializer,
            @Nonnull final TypeSerializer<byte[]> valueSerializer,
            @Nonnull DataInputDeserializer dataInputView) {
            this.db = db;

            this.userKeyOffset = userKeyOffset;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            this.rawKeyBytes = rawKeyBytes;
            this.rawValueBytes = rawValueBytes;
            this.deleted = false;
            this.dataInputView = dataInputView;
        }

        public void remove() {
            deleted = true;
            rawValueBytes = null;

            try {
                db.delete(columnFamily, writeOptions, rawKeyBytes);
            } catch (RocksDBException e) {
                throw new FlinkRuntimeException("Error while removing data from RocksDB.", e);
            }
        }

        @Override
        public byte[] getKey() {
            if (userKey == null) {
                try {
                    userKey =
                        deserializeUserKey(
                            dataInputView, userKeyOffset, rawKeyBytes, keySerializer);
                } catch (IOException e) {
                    throw new FlinkRuntimeException("Error while deserializing the user key.", e);
                }
            }

            return userKey;
        }

        @Override
        public byte[] getValue() {
            if (deleted) {
                return null;
            } else {
                if (userValue == null) {
                    try {
                        userValue =
                            deserializeUserValue(dataInputView, rawValueBytes, valueSerializer);
                    } catch (IOException e) {
                        throw new FlinkRuntimeException(
                            "Error while deserializing the user value.", e);
                    }
                }

                return userValue;
            }
        }

        @Override
        public byte[] setValue(byte[] value) {
            if (deleted) {
                throw new IllegalStateException("The value has already been deleted.");
            }

            byte[] oldValue = getValue();

            try {
                userValue = value;
                rawValueBytes = serializeValueNullSensitive(value, valueSerializer);

                db.put(columnFamily, writeOptions, rawKeyBytes, rawValueBytes);
            } catch (IOException | RocksDBException e) {
                throw new FlinkRuntimeException("Error while putting data into RocksDB.", e);
            }

            return oldValue;
        }
    }

    /**
     * An auxiliary utility to scan all entries under the given key.
     */
    private abstract class RocksDBMapIterator<T> implements Iterator<T> {

        private static final int CACHE_SIZE_LIMIT = 128;

        /**
         * The db where data resides.
         */
        private final RocksDB db;

        /**
         * The prefix bytes of the key being accessed. All entries under the same key have the same
         * prefix, hence we can stop iterating once coming across an entry with a different prefix.
         */
        @Nonnull
        private final byte[] keyPrefixBytes;

        /**
         * True if all entries have been accessed or the iterator has come across an entry with a
         * different prefix.
         */
        private boolean expired = false;

        /**
         * A in-memory cache for the entries in the rocksdb.
         */
        private ArrayList<RocksDBMapEntry> cacheEntries = new ArrayList<>();

        /**
         * The entry pointing to the current position which is last returned by calling {@link
         * #nextEntry()}.
         */
        private RocksDBMapEntry currentEntry;

        private int cacheIndex = 0;

        private final TypeSerializer<byte[]> keySerializer;
        private final TypeSerializer<byte[]> valueSerializer;
        private final DataInputDeserializer dataInputView;

        RocksDBMapIterator(
            final RocksDB db,
            final byte[] keyPrefixBytes,
            final TypeSerializer<byte[]> keySerializer,
            final TypeSerializer<byte[]> valueSerializer,
            DataInputDeserializer dataInputView) {

            this.db = db;
            this.keyPrefixBytes = keyPrefixBytes;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.dataInputView = dataInputView;
        }

        @Override
        public boolean hasNext() {
            loadCache();

            return (cacheIndex < cacheEntries.size());
        }

        @Override
        public void remove() {
            if (currentEntry == null || currentEntry.deleted) {
                throw new IllegalStateException(
                    "The remove operation must be called after a valid next operation.");
            }

            currentEntry.remove();
        }

        final RocksDBMapEntry nextEntry() {
            loadCache();

            if (cacheIndex == cacheEntries.size()) {
                if (!expired) {
                    throw new IllegalStateException();
                }

                return null;
            }

            this.currentEntry = cacheEntries.get(cacheIndex);
            cacheIndex++;

            return currentEntry;
        }

        private void loadCache() {
            if (cacheIndex > cacheEntries.size()) {
                throw new IllegalStateException();
            }

            // Load cache entries only when the cache is empty and there still exist unread entries
            if (cacheIndex < cacheEntries.size() || expired) {
                return;
            }

            // use try-with-resources to ensure RocksIterator can be release even some runtime
            // exception
            // occurred in the below code block.
            try (RocksIteratorWrapper iterator =
                     RocksDBOperationUtils.getRocksIterator(
                         db, columnFamily, backend.getReadOptions())) {

                /*
                 * The iteration starts from the prefix bytes at the first loading. After #nextEntry() is called,
                 * the currentEntry points to the last returned entry, and at that time, we will start
                 * the iterating from currentEntry if reloading cache is needed.
                 */
                byte[] startBytes =
                    (currentEntry == null ? keyPrefixBytes : currentEntry.rawKeyBytes);

                cacheEntries.clear();
                cacheIndex = 0;

                iterator.seek(startBytes);

                /*
                 * If the entry pointing to the current position is not removed, it will be the first entry in the
                 * new iterating. Skip it to avoid redundant access in such cases.
                 */
                if (currentEntry != null && !currentEntry.deleted) {
                    iterator.next();
                }

                while (true) {
                    if (!iterator.isValid()
                        || !startWithKeyPrefix(keyPrefixBytes, iterator.key())) {
                        expired = true;
                        break;
                    }

                    if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
                        break;
                    }

                    RocksDBMapEntry entry =
                        new RocksDBMapEntry(
                            db,
                            keyPrefixBytes.length,
                            iterator.key(),
                            iterator.value(),
                            keySerializer,
                            valueSerializer,
                            dataInputView);

                    cacheEntries.add(entry);

                    iterator.next();
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
        StateDescriptor<S, SV> stateDesc,
        Tuple2<VectorColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
            registerResult,
        RocksDBKeyedStateBackend<K> backend) throws RocksDBException {
        return (IS)
            new RocksDBMockVectorMapState<>(
                registerResult.f0,
                registerResult.f1.getNamespaceSerializer(),
                (TypeSerializer<Map<byte[], byte[]>>) registerResult.f1.getStateSerializer(),
                (Map<byte[], byte[]>) stateDesc.getDefaultValue(),
                backend);
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
        StateDescriptor<S, SV> stateDesc,
        Tuple2<VectorColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
            registerResult,
        IS existingState) {
        return (IS)
            ((RocksDBMockVectorMapState<K, N>) existingState)
                .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                .setValueSerializer(
                    (TypeSerializer<Map<byte[], byte[]>>)
                        registerResult.f1.getStateSerializer())
                .setDefaultValue((Map<byte[], byte[]>) stateDesc.getDefaultValue());
    }

    /**
     * RocksDB map state specific byte value transformer wrapper.
     *
     * <p>This specific transformer wrapper checks the first byte to detect null user value entries
     * and if not null forward the rest of byte array to the original byte value transformer.
     */
    static class StateSnapshotTransformerWrapper implements StateSnapshotTransformer<byte[]> {
        private static final byte[] NULL_VALUE;
        private static final byte NON_NULL_VALUE_PREFIX;

        static {
            DataOutputSerializer dov = new DataOutputSerializer(1);
            try {
                dov.writeBoolean(true);
                NULL_VALUE = dov.getCopyOfBuffer();
                dov.clear();
                dov.writeBoolean(false);
                NON_NULL_VALUE_PREFIX = dov.getSharedBuffer()[0];
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                    "Failed to serialize boolean flag of map user null value", e);
            }
        }

        private final StateSnapshotTransformer<byte[]> elementTransformer;
        private final DataInputDeserializer div;

        StateSnapshotTransformerWrapper(StateSnapshotTransformer<byte[]> originalTransformer) {
            this.elementTransformer = originalTransformer;
            this.div = new DataInputDeserializer();
        }

        @Override
        @Nullable
        public byte[] filterOrTransform(@Nullable byte[] value) {
            if (value == null || isNull(value)) {
                return NULL_VALUE;
            } else {
                // we have to skip the first byte indicating null user value
                // TODO: optimization here could be to work with slices and not byte arrays
                // and copy slice sub-array only when needed
                byte[] woNullByte = Arrays.copyOfRange(value, 1, value.length);
                byte[] filteredValue = elementTransformer.filterOrTransform(woNullByte);
                if (filteredValue == null) {
                    filteredValue = NULL_VALUE;
                } else if (filteredValue != woNullByte) {
                    filteredValue = prependWithNonNullByte(filteredValue, value);
                } else {
                    filteredValue = value;
                }
                return filteredValue;
            }
        }

        private boolean isNull(byte[] value) {
            try {
                div.setBuffer(value, 0, 1);
                return div.readBoolean();
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                    "Failed to deserialize boolean flag of map user null value", e);
            }
        }

        private static byte[] prependWithNonNullByte(byte[] value, byte[] reuse) {
            int len = 1 + value.length;
            byte[] result = reuse.length == len ? reuse : new byte[len];
            result[0] = NON_NULL_VALUE_PREFIX;
            System.arraycopy(value, 0, result, 1, value.length);
            return result;
        }
    }
}
