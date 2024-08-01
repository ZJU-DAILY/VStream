package cn.edu.zju.daily.lsh;

import cn.edu.zju.daily.data.vector.FloatVector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class L2HashFamilyTest {


  /** Test the distribution of LSH family. */
  @Test
  void testDistribution() {

    List<FloatVector> vectors = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      vectors.add(FloatVector.getRandom(i, 128));
    }

    L2HashFamily family = new L2HashFamily(128, 10, 5, new Random(324));
    for (FloatVector vector : vectors) {
      int[] hashValues = family.hash(vector);
      System.out.println(L2HashFamily.getNodeId(hashValues, 10));
    }
  }
}
