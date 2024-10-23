package cn.edu.zju.daily.lsh;

import cn.edu.zju.daily.data.vector.VectorData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * A family of locality-sensitive hashing functions.
 *
 * @author auroflow
 */
public class L2HashFamily implements LSHashFamily {

    private final List<L2Hash> hashFunctions;

    /**
     * Creates a family of locality-sensitive hashing functions.
     *
     * @param dim dimension of the vector
     * @param k number of hash functions
     * @param r width of hash bucket (see paper)
     * @param random
     */
    public L2HashFamily(int dim, int k, float r, Random random) {
        hashFunctions = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            hashFunctions.add(new L2Hash(dim, r, new Random(random.nextLong())));
        }
    }

    public L2HashFamily(int dim, int k, float r) {
        hashFunctions = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            hashFunctions.add(new L2Hash(dim, r));
        }
    }

    public int getNumHashFunctions() {
        return hashFunctions.size();
    }

    /**
     * Hashes a vector.
     *
     * @param vector
     * @return hash values
     */
    public int[] hash(VectorData vector) {
        int[] hashValues = new int[hashFunctions.size()];
        for (int i = 0; i < hashFunctions.size(); i++) {
            hashValues[i] = hashFunctions.get(i).hash(vector);
        }
        return hashValues;
    }

    /**
     * Get the node number according to the hash values.
     *
     * @param hashValues hash values
     * @param k number of nodes
     * @return node number
     * @deprecated Inferior method. Do not use in new code.
     */
    public static int getNodeId(int[] hashValues, int k) {
        return Math.floorMod(Arrays.hashCode(hashValues), k);
    }
}
