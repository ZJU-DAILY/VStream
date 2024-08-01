package cn.edu.zju.daily.data.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FloatVectorIterator implements Iterator<FloatVector>, Iterable<FloatVector> {

    private int count = 0;

    private final FvecIterator it;

    private final int total;

    public FloatVectorIterator(FvecIterator it) {
        this(it, Integer.MAX_VALUE);
    }

    public FloatVectorIterator(FvecIterator it, int total) {
        this.it = it;
        this.total = total;
    }

    @Override
    public boolean hasNext() {
        return count < total && it.hasNext();
    }

    @Override
    public FloatVector next() {
        if (count >= total) {
            throw new NoSuchElementException();
        }
        float[] arr = it.next();
        return new FloatVector(count++, arr);
    }

    public static FloatVectorIterator fromFile(String filename) throws IOException {
        return new FloatVectorIterator(FvecIterator.fromFile(filename), Integer.MAX_VALUE);
    }

    public static FloatVectorIterator fromFile(String filename, int numLoops) throws IOException {
        return new FloatVectorIterator(FvecIterator.fromFile(filename, numLoops), Integer.MAX_VALUE);
    }

    public static FloatVectorIterator fromFile(String filename, int numLoops, int total) throws IOException {

        return new FloatVectorIterator(FvecIterator.fromFile(filename, numLoops), total);
    }

    @Override
    public Iterator<FloatVector> iterator() {
        return this;
    }
}
