package fade.util;

import java.util.Arrays;
import java.util.Random;

public class KmerTool {

    public static final byte[] bitmask, bitmask_comp, bitmask_comp_char, bitmask_char;

    static {
        bitmask = new byte[117];

        bitmask['A'] = 0;
        bitmask['C'] = 1;
        bitmask['G'] = 2;
        bitmask['T'] = 3;
        bitmask['a'] = 0;
        bitmask['c'] = 1;
        bitmask['g'] = 2;
        bitmask['t'] = 3;

        bitmask_comp = new byte[117];

        bitmask_comp['A'] = 3;
        bitmask_comp['C'] = 2;
        bitmask_comp['G'] = 1;
        bitmask_comp['T'] = 0;
        bitmask_comp['a'] = 3;
        bitmask_comp['c'] = 2;
        bitmask_comp['g'] = 1;
        bitmask_comp['t'] = 0;

        bitmask_comp_char = new byte[117];

        bitmask_comp_char['A'] = 'T';
        bitmask_comp_char['C'] = 'G';
        bitmask_comp_char['G'] = 'C';
        bitmask_comp_char['T'] = 'A';
        bitmask_comp_char['a'] = 'T';
        bitmask_comp_char['c'] = 'G';
        bitmask_comp_char['g'] = 'C';
        bitmask_comp_char['t'] = 'A';

        bitmask_char = new byte[4];

        bitmask_char[0] = 'A';
        bitmask_char[1] = 'C';
        bitmask_char[2] = 'G';
        bitmask_char[3] = 'T';
    }

    private long kmer, kmerRev;
    private byte[] sequence;
    private int k, offset;
    private long longmask;
    private long[] bitmask_revComp;

    public KmerTool(byte[] sequence, int k) {
        this.sequence = sequence;
        this.k = k;
        longmask = (long) (Math.pow(2, 2*k) - 1);
        bitmask_revComp = new long[117];

        bitmask_revComp['A'] = (long) Math.pow(2, 2 * (k - 1)) + (long) Math.pow(2, 2 * (k - 1) + 1); // T
        bitmask_revComp['T'] = 0; // A
        bitmask_revComp['G'] = (long) Math.pow(2, 2 * (k - 1)); // C
        bitmask_revComp['C'] = (long) Math.pow(2, 2 * (k - 1) + 1); // G
        bitmask_revComp['a'] = (long) Math.pow(2, 2 * (k - 1)) + (long) Math.pow(2, 2 * (k - 1) + 1); // T
        bitmask_revComp['t'] = 0; // A
        bitmask_revComp['g'] = (long) Math.pow(2, 2 * (k - 1)); // C
        bitmask_revComp['c'] = (long) Math.pow(2, 2 * (k - 1) + 1); // G

        firstKmer();
    }

    private void firstKmer() {
        for (int i = 0; i < k-1; i++) {
            kmer <<= 2;
            kmerRev >>>= 2;

            kmer |= bitmask[sequence[i]];
            kmerRev |= bitmask_revComp[sequence[i]];
        }

        offset = k-1;
    }

    public long nextKmerCan() {
        next();

        return Math.min(kmer, kmerRev);
    }

    public long nextKmer() {
        next();

        return kmer;
    }

    private void next() {
        kmer <<= 2;
        kmerRev >>>= 2;

        kmer |= bitmask[sequence[offset]];
        kmerRev |= (((long) bitmask_comp[sequence[offset]]) << 2*(k-1));

        offset++;

        kmer &= longmask;
        kmerRev &= longmask;
    }

    public static byte[] bytes2bytesCan(byte[] kmer) {

        return bytes2bytesCan(kmer, 0, kmer.length);
    }

    public static byte[] bytes2bytesCan(byte[] sequence, int offset, int k) {

        byte[] bytes_rev = new byte[k];
        int last = offset + k - 1;

        for (int i = 0; i < k; i++) {

            bytes_rev[i] = bitmask_comp[sequence[last - i]];
        }

        for (int i = 0; i < k; i++)
            if (bytes_rev[i] < sequence[offset + i])
                return bytes_rev;
            else if (sequence[offset + i] < bytes_rev[i])
                return Arrays.copyOfRange(sequence, offset, offset+k);

        return bytes_rev;
    }

    public static long bytes2long(byte[] kmer) {
        return bytes2long(kmer, 0, kmer.length);
    }

    public static long bytes2long(byte[] sequence, int offset, int k) {

        long l = 0;

        for (int i = offset; i < offset + k; i++) {
            l <<= 2;
            l |= bitmask[sequence[i]];
        }

        return l;
    }

    public static long bytes2longCan(byte[] kmer) {
        return bytes2longCan(kmer, 0, kmer.length);
    }

    public static long bytes2longCan(byte[] sequence, int offset, int k) {

        long l = 0;
        long l_rev = 0;
        int last = offset + k - 1;

        for (int i = offset; i < offset + k; i++) {

            l <<= 2;
            l_rev <<= 2;

            l |= bitmask[sequence[i]];
            l_rev |= bitmask_comp[sequence[last - i]];
        }

        return Math.min(l, l_rev);
    }

    public static byte[] long2bytes(long l, int k) {

        byte[] kmer = new byte[k];

        int res, i = -1;

        while (++i < k) {
            res = (int) (l & 0b11);
            kmer[k-1 - i] = bitmask_char[res];
            l >>= 2;
        }

        return kmer;
    }

    public static String long2string(long l, int k) {

        return new String(long2bytes(l, k));
    }

    public static byte[] generate(int len) {
        Random rnd = new Random(42);
        byte[] sequence = new byte[len];

        for (int i = 0; i < len; i++)
            sequence[i] = bitmask_char[rnd.nextInt(4)];

        return sequence;
    }
}
