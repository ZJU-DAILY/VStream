// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public interface MutableVectorColumnFamilyOptionsInterface<T extends MutableVectorColumnFamilyOptionsInterface<T>>
                extends MutableColumnFamilyOptionsInterface<T> {
        T setMaxElements(long maxElements);

        long maxElements();

        T setM(long M);

        long M();

        T setEfConstruction(long efConstruction);

        long efConstruction();

        T setRandomSeed(long randomSeed);

        long randomSeed();

        T setVisitListPoolSize(long visitListPoolSize);

        long visitListPoolSize();

        T setTerminationThreshold(float terminationThreshold);

        float terminationThreshold();

        T setTerminationWeight(float terminationWeight);

        float terminationWeight();

        T setTerminationLowerBound(float terminationWeight);

        float terminationLowerBound();
}
