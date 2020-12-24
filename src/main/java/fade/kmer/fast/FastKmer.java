package fade.kmer.fast;

import fade.util.Statistic;

import java.util.Arrays;

public class FastKmer extends Statistic {

    public final FastKmerUtils.Kmer kmer;

    public FastKmer(FastKmerUtils.Kmer kmer) {
        this.kmer = kmer;
    }

    @Override
    public int getHashCode() { // todo: to implement
        return Arrays.hashCode(kmer.toByteArray());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FastKmer)) return false;

        return Arrays.equals(((FastKmer)obj).kmer.toByteArray(), kmer.toByteArray());
    }

    @Override
    public String toString() {
        return new String(kmer.toByteArray());
    }
}
