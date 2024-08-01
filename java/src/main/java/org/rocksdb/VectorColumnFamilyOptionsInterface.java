package org.rocksdb;

public interface VectorColumnFamilyOptionsInterface<T extends VectorColumnFamilyOptionsInterface<T>>
extends ColumnFamilyOptionsInterface<T> {
    T setDim(long dim);

    long dim();

    T setSpace(SpaceType spaceType);

    byte space();

    T setAllowReplaceDeleted(boolean allowReplaceDeleted);

    boolean allowReplaceDeleted();
}
