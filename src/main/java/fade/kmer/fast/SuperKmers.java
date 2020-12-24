package fade.kmer.fast;

import fade.util.Data;

import java.util.List;

public class SuperKmers implements Data {
    List<FastKmerUtils.Kmer> superkmers;

    public SuperKmers(List<FastKmerUtils.Kmer> kmers) {
        superkmers = kmers;
    }

    @Override
    public long size() {
        return superkmers.size();
    }
}
