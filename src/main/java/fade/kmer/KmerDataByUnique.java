package fade.kmer;

import fade.util.Data;

public class KmerDataByUnique implements Data {
    public final Kmer kmer;
    public final long count;

    public KmerDataByUnique(Kmer kmer, long count) {
        this.kmer = kmer;
        this.count = count;
    }
}
