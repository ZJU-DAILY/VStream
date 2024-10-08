package cn.edu.zju.daily.util;

import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import cn.edu.zju.daily.data.vector.FvecIterator;
import cn.edu.zju.daily.data.vector.HDFSVectorParser;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;

/** @author shenghao */
public class DataUploader {

    private final String sourcePath;
    private final int loop;
    private final HadoopFileHelper hadoopFileHelper;
    private final int limit;
    private final double deleteRatio;
    private final Random random;

    public DataUploader(
            String sourcePath,
            String hdfsAddress,
            String hdfsUser,
            String hdfsPath,
            double deleteRatio,
            int loop,
            int limit)
            throws IOException {
        this.sourcePath = sourcePath;
        this.loop = loop;
        this.hadoopFileHelper = new HadoopFileHelper(hdfsAddress, hdfsPath, hdfsUser);
        this.limit = limit;
        this.deleteRatio = deleteRatio;
        this.random = new Random();
    }

    public void convert() throws IOException {
        HDFSVectorParser parser = new HDFSVectorParser();
        FvecIterator.InputType inputType;

        FloatVectorIterator it = FloatVectorIterator.fromFile(sourcePath, loop, limit);
        Pair<OutputStream, BufferedWriter> outputStreamBufferedWriterTuple2 =
                hadoopFileHelper.beginWrite();

        long i = 0;

        if (deleteRatio == 0) {
            while (it.hasNext()) {
                hadoopFileHelper.writeString(
                        outputStreamBufferedWriterTuple2.getRight(), parser.unparseBare(it.next()));
                if (i % 500_000 == 0) {
                    System.out.println(i + " vectors converted.");
                }
                i += 1;
            }
        } else {
            while (it.hasNext()) {
                boolean delete = random.nextDouble() < deleteRatio;
                if (delete) {
                    long id = random.nextInt(it.nextId());
                    hadoopFileHelper.writeString(
                            outputStreamBufferedWriterTuple2.getRight(), parser.unparseDelete(id));
                } else {
                    hadoopFileHelper.writeString(
                            outputStreamBufferedWriterTuple2.getRight(),
                            parser.unparseInsert(it.next()));
                }
                if (i % 500_000 == 0) {
                    System.out.println(i + " vectors converted.");
                }
                i += 1;
            }
        }

        hadoopFileHelper.endWrite(
                outputStreamBufferedWriterTuple2.getRight(),
                outputStreamBufferedWriterTuple2.getLeft());
        System.out.println("All " + i + " vectors converted.");
    }

    public static void main(String[] args) {
        if (args.length == 1 && args[0].equals("-h")) {
            System.out.println("Upload vector datasets to HDFS.");
            System.out.println();
            System.out.println(
                    "Usage: DataUploader <sourcePath> <hdfsAddress> <hdfsUser> <hdfsPath> <deleteRatio> [<loop> <limit>]");
            System.out.println();
            System.out.println("Arguments:");
            System.out.println("  sourcePath   Path to the source file.");
            System.out.println("  hdfsAddress  HDFS address.");
            System.out.println("  hdfsUser     HDFS user name.");
            System.out.println("  hdfsPath     Path to the target file on HDFS.");
            System.out.println(
                    "  deleteRatio  Ratio of deletion markers. Must be between 0 and 1. Delete markers are not counted in the limit.");
            System.out.println(
                    "  loop         Number of loops to read the source file. Default is 1.");
            System.out.println(
                    "  limit        Number of vectors to read in each loop. Default is unlimited.");
            System.exit(0);
        }

        if (args.length < 5 || args.length > 7) {
            System.err.println("Run 'DataUploader -h' for help.");
            System.err.println(
                    "Usage: DataUploader <sourcePath> <hdfsAddress> <hdfsUser> <hdfsPath> <deleteRatio> [<loop> <limit>]");
            System.exit(1);
        }
        String sourcePath = args[0];
        String hdfsAddress = args[1];
        String hdfsUser = args[2];
        String hdfsPath = args[3];
        double deleteRatio = Double.parseDouble(args[4]);
        int loop = 1;
        int limit = Integer.MAX_VALUE;
        if (args.length >= 6) {
            loop = Integer.parseInt(args[5]);
        }
        if (args.length == 7) {
            limit = Integer.parseInt(args[6]);
        }
        try {
            DataUploader uploader =
                    new DataUploader(
                            sourcePath, hdfsAddress, hdfsUser, hdfsPath, deleteRatio, loop, limit);
            uploader.convert();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
