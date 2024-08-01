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

package org.apache.flink.contrib.streaming.vstate.ttl;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.VectorColumnFamilyOptions;

import javax.annotation.Nonnull;

/** RocksDB compaction filter utils for state with TTL. */
public class RocksDbTtlCompactFiltersManager {

    public RocksDbTtlCompactFiltersManager(TtlTimeProvider ttlTimeProvider) {

    }

    public void setAndRegisterCompactFilterIfStateTtl(
            @Nonnull RegisteredStateMetaInfoBase metaInfoBase,
            @Nonnull ColumnFamilyOptions options) {

    }

    public void setAndRegisterCompactFilterIfStateTtl(
        @Nonnull RegisteredStateMetaInfoBase metaInfoBase,
        @Nonnull VectorColumnFamilyOptions options) {

    }

    public void configCompactFilter(
            @Nonnull StateDescriptor<?, ?> stateDesc, TypeSerializer<?> stateSerializer) {

    }

    public void disposeAndClearRegisteredCompactionFactories() {

    }
}
