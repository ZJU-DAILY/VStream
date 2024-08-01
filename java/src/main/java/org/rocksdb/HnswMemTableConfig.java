// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

/**
 * The config for hnsw memtable representation.
 */
public class HnswMemTableConfig extends MemTableConfig {
    private long dim_;
    private SpaceType space_ = SpaceType.L2;
    private long max_elements_ = 100_000;
    private long M_ = 16;
    private long ef_construction_ = 200;
    private long random_seed_ = 100;
    private long visit_list_pool_size_ = 1;
    private boolean allow_replace_deleted = true;

    public HnswMemTableConfig(long dim) {dim_ = dim;}

    public HnswMemTableConfig setDim(final long dim) {
        dim_ = dim;
        return this;
    }

    public long dim() {return dim_;}

    public HnswMemTableConfig setSpace(final SpaceType space) {
        space_ = space;
        return this;
    }

    public SpaceType space() {return space_;}

    public HnswMemTableConfig setMaxElements(final long max_elements) {
        max_elements_ = max_elements;
        return this;
    }

    public long maxElements() {return max_elements_;}

    public HnswMemTableConfig setM(final long M) {
        M_ = M;
        return this;
    }

    public long M() {return M_;}

    public HnswMemTableConfig setEfConstruction(final long ef_construction) {
        ef_construction_ = ef_construction;
        return this;
    }

    public long efConstruction() {return ef_construction_;}

    public HnswMemTableConfig setRandomSeed(final long random_seed) {
        random_seed_ = random_seed;
        return this;
    }

    public long randomSeed() {return random_seed_;}

    public HnswMemTableConfig setVisitListPoolSize(final long visit_list_pool_size) {
        visit_list_pool_size_ = visit_list_pool_size;
        return this;
    }

    public long visitListPoolSize() {return visit_list_pool_size_;}

    public HnswMemTableConfig setAllowReplaceDeleted(final boolean allow_replace_deleted) {
        this.allow_replace_deleted = allow_replace_deleted;
        return this;
    }

    public boolean allowReplaceDeleted() {return allow_replace_deleted;}

    @Override
    protected long newMemTableFactoryHandle() {
        return newMemTableFactoryHandle0(dim_, SpaceType.toByte(space_), max_elements_, M_, ef_construction_,
                                         random_seed_,
                                         visit_list_pool_size_, allow_replace_deleted);
    }

    private native long newMemTableFactoryHandle0(long dim, byte spaceType, long maxElements, long M,
                                                  long efConstruction, long randomSeed, long visitListPoolSize,
                                                  boolean allowReplaceDeleted)
            throws IllegalArgumentException;
}
