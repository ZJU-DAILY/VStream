package cn.edu.zju.daily.function.partitioner.sax;

import java.math.BigInteger;
import net.seninp.jmotif.sax.TSProcessor;
import net.seninp.jmotif.sax.alphabet.Alphabet;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;

public class SAX {
    private final int paaSize;
    private final int width;
    private final int cardinality;
    private final TSProcessor tsProcessor;
    private final double normalizationThreshold;
    private final Alphabet alphabet;

    /**
     * Constructor.
     *
     * @param paaSize length of the encoded SAX representation
     * @param width the bit width of each element
     * @param normalizationThreshold normalization threshold
     */
    public SAX(int paaSize, int width, double normalizationThreshold) {
        this.paaSize = paaSize;
        this.width = width;
        this.cardinality = (int) Math.pow(2, width);
        this.tsProcessor = new TSProcessor();
        this.normalizationThreshold = normalizationThreshold;
        this.alphabet = new NormalAlphabet();
    }

    private int[] getSaxes(float[] vector) throws Exception {
        double[] ts = new double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            ts[i] = vector[i];
        }

        double[] normalizedTS = tsProcessor.znorm(ts, normalizationThreshold);
        double[] paa = tsProcessor.paa(normalizedTS, paaSize);
        return this.tsProcessor.ts2Index(paa, alphabet.getCuts(cardinality));
    }

    /**
     * Encode a float vector using SAX and gray code.
     *
     * @param vector input vector
     * @return encoded word
     */
    public BigInteger encodeBig(float[] vector) throws Exception {
        int[] saxes = getSaxes(vector);

        BigInteger saxWord = BigInteger.ZERO;
        for (int sax : saxes) {
            saxWord = saxWord.shiftLeft(width);
            saxWord = saxWord.add(BigInteger.valueOf(sax));
        }

        // gray code
        saxWord = saxWord.xor(saxWord.shiftRight(1));
        return saxWord;
    }

    public int encode(float[] vector) throws Exception {
        if (paaSize * width >= Integer.SIZE) {
            throw new IllegalArgumentException("The size of the SAX word is too large.");
        }

        int[] saxes = getSaxes(vector);
        int saxWord = 0;
        for (int sax : saxes) {
            saxWord = (saxWord << width) + sax;
        }

        // gray code
        saxWord = saxWord ^ (saxWord >> 1);
        return saxWord;
    }

    public int getMaxWord() {
        if (paaSize * width >= Integer.SIZE) {
            throw new IllegalArgumentException("The size of the SAX word is too large.");
        }
        return 1 << (paaSize * width);
    }

    public BigInteger getMaxWordBig() {
        return BigInteger.ONE.shiftLeft(paaSize * width);
    }
}
