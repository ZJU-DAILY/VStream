package cn.edu.zju.daily.lsh;

import cn.edu.zju.daily.data.vector.FloatVector;

import java.io.Serializable;
import java.util.Random;

/**
 * Implementation of LSH based on p-stable distributions.
 *
 * @see <a href="https://dl.acm.org/doi/10.1145/997817.997857">Locality-sensitive hashing scheme based on p-stable distributions</a>
 * @author auroflow
 */
public class L2Hash implements LSHash {

  private final FloatVector a;
  private final float b;
  private final float r;

  public L2Hash(int dim, float r) {
    this(dim, r, new Random());
  }

  public L2Hash(int dim, float r, Random random) {
    this.r = r;
    this.b = random.nextFloat() * r;

    float[] arr = new float[dim];
    for (int i = 0; i < dim; i++) {
      arr[i] = (float) random.nextGaussian();
    }
    this.a = new FloatVector(-1, arr);
  }

  public int hash(FloatVector vector) {
    float hashValue = (a.dot(vector) + b) / r;
    return (int) Math.floor(hashValue);
  }
}
