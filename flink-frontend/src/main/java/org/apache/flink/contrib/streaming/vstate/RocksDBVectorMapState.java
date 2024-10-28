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

import static org.apache.flink.util.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
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
import org.rocksdb.*;

/**
 * {@link MapState} implementation that stores state in RocksDB vector backend.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 */
@Slf4j
public class RocksDBVectorMapState<K, N> extends AbstractRocksDBState<K, N, Map<byte[], byte[]>>
        implements InternalMapState<K, N, byte[], byte[]> {

    /** Serializer for the keys and values. */
    private TypeSerializer<byte[]> userKeySerializer;

    private TypeSerializer<byte[]> userValueSerializer;

    private final VectorSearchOptions searchOptions;

    private final VectorColumnFamilyHandle vectorColumnFamily;

    /**
     * Creates a new {@code RocksDBVectorMapState}.
     *
     * @param vectorColumnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBVectorMapState(
            VectorColumnFamilyHandle vectorColumnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<Map<byte[], byte[]>> valueSerializer,
            Map<byte[], byte[]> defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        super(null, namespaceSerializer, valueSerializer, defaultValue, backend);

        Preconditions.checkState(
                valueSerializer instanceof MapSerializer, "Unexpected serializer type.");

        this.vectorColumnFamily = vectorColumnFamily;
        MapSerializer<byte[], byte[]> castedMapSerializer =
                (MapSerializer<byte[], byte[]>) valueSerializer;
        this.userKeySerializer = castedMapSerializer.getKeySerializer();
        this.userValueSerializer = castedMapSerializer.getValueSerializer();
        // this.searchOptions = new VectorSearchOptions(backend.getVectorSearchOptions());
        this.searchOptions = backend.getVectorSearchOptions();
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
     * <p>If userKey.length == 1, set searchOptions.triggerSort() to true. If userKey.length == 2,
     * set searchOptions.triggerSort() to false. Otherwise, treat userKey as the query vector.
     *
     * @param userKey The byte array of the vector to search for.
     * @return the result byte array.
     * @throws IOException
     * @throws RocksDBException
     */
    @Override
    public byte[] get(byte[] userKey) throws IOException, RocksDBException {

        if (userKey.length == 1) {
            searchOptions.setTriggerSort(true);
            return null;
        } else if (userKey.length == 2) {
            searchOptions.setTriggerSort(false);
            return null;
        } else {
            long leastEventTime =
                    ByteBuffer.wrap(userKey, 0, Long.BYTES)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getLong();
            searchOptions.setTs(Math.max(0, leastEventTime));
            return backend.db.vectorSearch(
                    vectorColumnFamily,
                    searchOptions,
                    Arrays.copyOfRange(userKey, Long.BYTES, userKey.length));
        }
    }

    /**
     * Performs vector insert.
     *
     * @param userKey The byte array of the vector ID (long type) to insert.
     * @param userValue The byte array of the vector to insert.
     * @throws IOException
     * @throws RocksDBException
     */
    @Override
    public void put(byte[] userKey, byte[] userValue) throws IOException, RocksDBException {

        backend.db.put(vectorColumnFamily, writeOptions, userKey, userValue);
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

    /**
     * Performs vector delete.
     *
     * @param userKey The byte array containing the vector ID to remove.
     * @throws IOException
     * @throws RocksDBException
     */
    @Override
    public void remove(byte[] userKey) throws IOException, RocksDBException {
        backend.db.delete(vectorColumnFamily, writeOptions, userKey);
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
    protected RocksDBVectorMapState<K, N> setValueSerializer(
            TypeSerializer<Map<byte[], byte[]>> valueSerializer) {
        super.setValueSerializer(valueSerializer);
        MapSerializer<byte[], byte[]> castedMapSerializer =
                (MapSerializer<byte[], byte[]>) valueSerializer;
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

        final MapSerializer<byte[], byte[]> serializer =
                (MapSerializer<byte[], byte[]>) safeValueSerializer;

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

    /** A map entry in RocksDBVectorMapState. */
    private class RocksDBMapEntry implements Map.Entry<byte[], byte[]> {
        private final RocksDB db;

        /**
         * The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB with the
         * format #KeyGroup#Key#Namespace#UserKey.
         */
        private final byte[] rawKeyBytes;

        /** The raw bytes of the value stored in RocksDB. */
        private byte[] rawValueBytes;

        /** True if the entry has been deleted. */
        private boolean deleted;

        /**
         * The user key and value. The deserialization is performed lazily, i.e. the key and the
         * value is deserialized only when they are accessed.
         */
        private byte[] userKey;

        private byte[] userValue;

        /** The offset of User Key offset in raw key bytes. */
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

    /** An auxiliary utility to scan all entries under the given key. */
    private abstract class RocksDBMapIterator<T> implements Iterator<T> {

        private static final int CACHE_SIZE_LIMIT = 128;

        /** The db where data resides. */
        private final RocksDB db;

        /**
         * The prefix bytes of the key being accessed. All entries under the same key have the same
         * prefix, hence we can stop iterating once coming across an entry with a different prefix.
         */
        @Nonnull private final byte[] keyPrefixBytes;

        /**
         * True if all entries have been accessed or the iterator has come across an entry with a
         * different prefix.
         */
        private boolean expired = false;

        /** A in-memory cache for the entries in the rocksdb. */
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
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBVectorMapState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        (TypeSerializer<Map<byte[], byte[]>>)
                                registerResult.f1.getStateSerializer(),
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
                ((RocksDBVectorMapState<K, N>) existingState)
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
