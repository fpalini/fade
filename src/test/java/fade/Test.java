package fade;

import com.google.common.primitives.Longs;
import fade.mash.MashData;
import fade.mash.MashValue;
import fade.util.*;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import org.apache.orc.storage.common.util.Murmur3;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        byte[] bytes = "CGGGTATTTTGCTGAATGTGGGAA".getBytes(); // KmerTool.generate(50);
        int id = 0;
        int offset = 0;

        Chunk chunk = new Chunk(id, offset, bytes);
        System.out.println(new String(chunk.data));

        int k = 24;


        KmerTool tool = new KmerTool(chunk.data, k);

        System.out.println(KmerTool.long2string(tool.nextKmerCan(), k));

//        String s = "";
//        for (int i = 0; i <  chunk.data.length - (k-1); i++) {
//            System.out.println(s + KmerTool.long2string(tool.nextKmerCan(), k));
//            s += " ";
//        }

//        Configuration conf = new Configuration();
//        conf.setInt("k", 26);
//        conf.setInt("s", 30000);
//
//        long start = System.currentTimeMillis();
//
//        List<Tuple2<BinId, Tuple2<SequenceId, Data>>> out1 = Extractor.call(chunk, conf);
//
//        System.out.println("Time: " + ((System.currentTimeMillis() - start) / 1000.0) + "s");
//
//        List<Tuple2<Statistic, Tuple2<SequenceId, Value>>> out2 = Aggregator.call(out1, conf);
    }

    public static class Extractor {
        public static List<Tuple2<BinId, Tuple2<SequenceId, Data>>> call(Chunk chunk, Configuration conf) {
            int k = conf.getInt("k");
            int s = conf.getInt("s");
            int seqId = chunk.id;

            List<Tuple2<BinId, Tuple2<SequenceId, Data>>> output = new ArrayList<>(s);

            long[] sketches;

            if (k < 17)
                sketches = createSketches32(chunk, k, s);
            else
                sketches = createSketches64(chunk, k, s);

            output.add(new Tuple2<>(new BinId(seqId),new Tuple2<>(new SequenceId(seqId), new MashData(sketches))));

            return output;
        }
    }

    public static class Aggregator {
        public static List<Tuple2<Statistic, Tuple2<SequenceId, Value>>> call(List<Tuple2<BinId, Tuple2<SequenceId, Data>>> out1, Configuration conf) {
            int s = conf.getInt("s");

            long[] sketches = null;
            List<Tuple2<Statistic, Tuple2<SequenceId, Value>>> output = new ArrayList<>(s);

            for (Tuple2<BinId, Tuple2<SequenceId, Data>> t : out1) {
                long[] sketches1 = ((MashData)t._2._2).sketches;

                sketches = sketches == null? sketches1 : merge(sketches, sketches1, s);
            }

            output.add(new Tuple2<>(NoStatistic.getSingleton(), new Tuple2<>(new SequenceId(out1.get(0)._1.id), new MashValue(sketches))));

            return output;
        }
    }


    // additional functions

    private static long[] createSketches32(Chunk chunk, int k, int s) {
        int len = chunk.data.length;

        LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

        long hash;
        boolean takeCompRev;

        byte[] seqCompRev = new byte[len];

        for (int i = 0; i < len; i++)
            seqCompRev[i] = KmerTool.bitmask_comp_char[chunk.data[len-1-i]];

        for (int i = 0; i < Math.min(s, len - (k-1)); i++) {
            takeCompRev = false;

            for (int j = 0; j < k; j++) {
                byte kmer = chunk.data[i + j];
                byte kmerCR = seqCompRev[len-k-i + j];

                if (kmer < kmerCR)
                    break;
                else if (kmer > kmerCR) {
                    takeCompRev = true;
                    break;
                }
            }

            if (takeCompRev)
                hash = (int) org.apache.orc.util.Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
            else
                hash = (int) org.apache.orc.util.Murmur3.hash128(chunk.data, i, k, 42)[0];

            hashes.add(hash);
        }

        for (int i = s; i < len - (k-1); i++) {
            takeCompRev = false;

            for (int j = 0; j < k; j++) {
                byte kmer = chunk.data[i + j];
                byte kmerCR = seqCompRev[len-k-i + j];

                if (kmer < kmerCR)
                    break;
                else if (kmer > kmerCR) {
                    takeCompRev = true;
                    break;
                }
            }

            if (takeCompRev)
                hash = (int) org.apache.orc.util.Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
            else
                hash = (int) org.apache.orc.util.Murmur3.hash128(chunk.data, i, k, 42)[0];

            if (Long.compareUnsigned(hash, hashes.lastLong()) < 0) {
                hashes.remove(hashes.lastLong());
                hashes.add(hash);
            }
        }

        return hashes.toLongArray();
    }

    private static long[] createSketches64(Chunk chunk, int k, int s) {
        int len = chunk.data.length;

        LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

        long hash;
        boolean takeCompRev;

        byte[] seqCompRev = new byte[len];

        for (int i = 0; i < len; i++)
            seqCompRev[i] = KmerTool.bitmask_comp_char[chunk.data[len-1-i]];

        for (int i = 0; i < Math.min(s, len - (k-1)); i++) {
            takeCompRev = false;

            for (int j = 0; j < k; j++) {
                byte kmer = chunk.data[i + j];
                byte kmerCR = seqCompRev[len-k-i + j];

                if (kmer < kmerCR)
                    break;
                else if (kmer > kmerCR) {
                    takeCompRev = true;
                    break;
                }
            }

            if (takeCompRev)
                hash = org.apache.orc.util.Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
            else
                hash = org.apache.orc.util.Murmur3.hash128(chunk.data, i, k, 42)[0];

            hashes.add(hash);
        }

        for (int i = s; i < len - (k-1); i++) {
            takeCompRev = false;

            for (int j = 0; j < k; j++) {
                byte kmer = chunk.data[i + j];
                byte kmerCR = seqCompRev[len-k-i + j];

                if (kmer < kmerCR)
                    break;
                else if (kmer > kmerCR) {
                    takeCompRev = true;
                    break;
                }
            }

            if (takeCompRev)
                hash = org.apache.orc.util.Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
            else
                hash = org.apache.orc.util.Murmur3.hash128(chunk.data, i, k, 42)[0];

            if (Long.compareUnsigned(hash, hashes.lastLong()) < 0) {
                hashes.remove(hashes.lastLong());
                hashes.add(hash);
            }
        }

        return hashes.toLongArray();
    }

    private static long[] createSketches32long(Chunk chunk, int k, int s) {
        int len = chunk.data.length;

        LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

        long hash;

        KmerTool tool = new KmerTool(chunk.data, k);

        for (int i = 0; i < Math.min(s, len - (k-1)); i++) {
            long kmer = tool.nextKmerCan();

            hash = (int) Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

            hashes.add(hash);
        }

        for (int i = s; i < len - (k-1); i++) {
            long kmer = tool.nextKmerCan();

            hash = (int) Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

            if (Long.compareUnsigned(hash, hashes.lastLong()) < 0) {
                hashes.remove(hashes.lastLong());
                hashes.add(hash);
            }
        }

        return hashes.toLongArray();
    }

    private static long[] createSketches64long(Chunk chunk, int k, int s) {
        int len = chunk.data.length;

        LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

        long hash;

        KmerTool tool = new KmerTool(chunk.data, k);

        for (int i = 0; i < Math.min(s, len - (k-1)); i++) {
            long kmer = tool.nextKmerCan();

            hash = Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

            hashes.add(hash);
        }

        for (int i = s; i < len - (k-1); i++) {
            long kmer = tool.nextKmerCan();

            hash = Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

            if (Long.compareUnsigned(hash, hashes.lastLong()) < 0) {
                hashes.remove(hashes.lastLong());
                hashes.add(hash);
            }
        }

        return hashes.toLongArray();
    }

    public static long[] merge(long[] a, long[] b, int s) {
        long[] c = new long[s];
        int i = 0, j = 0, k = 0;

        while (k < s && i < a.length && j < b.length) {
            c[k++] = Long.compareUnsigned(a[i], b[j]) < 1 ? a[i++] : b[j++];

            if (i < a.length && c[k-1] == a[i])
                i++;

            if (j < b.length && c[k-1] == b[j])
                j++;
        }

        while (k < s && i < a.length) {
            c[k++] = a[i++];

            if (i < a.length && c[k-1] == a[i])
                i++;
        }

        while (k < s && j < b.length) {
            c[k++] = b[j++];

            if (j < b.length && c[k-1] == b[j])
                j++;
        }

        return Arrays.copyOf(c, k);
    }
}
