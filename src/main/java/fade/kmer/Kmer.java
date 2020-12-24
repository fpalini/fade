package fade.kmer;

import fade.util.Statistic;
import fade.util.KmerTool;

public class Kmer extends Statistic {

    public final long kmer;
    public final int k;

    public Kmer(long kmer, int k) {
        this.kmer = kmer;
        this.k = k;
    }

    @Override
    public int getHashCode() {
        return Long.hashCode(kmer);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Kmer)) return false;

        return ((Kmer)obj).kmer == kmer;
    }

    @Override
    public String toString() {
        return KmerTool.long2string(kmer, k);
    }
}
