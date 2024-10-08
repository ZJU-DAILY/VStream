package cn.edu.zju.daily.data.vector;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;

/** This class reads a vector file in SIFT format as {@link FloatVector}s. */
public class FloatVectorIterator implements Iterator<FloatVector>, Iterable<FloatVector> {

    private int count = 0;

    private final FvecIterator it;

    private final int total;

    private FloatVectorIterator(FvecIterator it, int total) {
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

    public int nextId() {
        return count;
    }

    /**
     * Creates a vector iterator.
     *
     * @param filename the file to read from
     * @return a new FloatVectorIterator
     * @throws IOException if the file cannot be read
     */
    public static FloatVectorIterator fromFile(String filename) throws IOException {
        return new FloatVectorIterator(FvecIterator.fromFile(filename), Integer.MAX_VALUE);
    }

    /**
     * Creates a vector iterator that loops the file for a specified number of times.
     *
     * @param filename the file to read from
     * @param numLoops the number of times to loop the file
     * @return a new FloatVectorIterator
     * @throws IOException if the file cannot be read
     */
    public static FloatVectorIterator fromFile(String filename, int numLoops) throws IOException {
        return new FloatVectorIterator(
                FvecIterator.fromFile(filename, numLoops), Integer.MAX_VALUE);
    }

    /**
     * Creates a vector iterator that loops the file for a specified number of times.
     *
     * @param filename the file to read from
     * @param numLoops the number of times to loop the file
     * @param total the total number of vectors to read
     * @return a new FloatVectorIterator
     * @throws IOException if the file cannot be read
     */
    public static FloatVectorIterator fromFile(String filename, int numLoops, int total)
            throws IOException {

        return new FloatVectorIterator(FvecIterator.fromFile(filename, numLoops), total);
    }

    @NotNull
    @Override
    public Iterator<FloatVector> iterator() {
        return this;
    }
}
