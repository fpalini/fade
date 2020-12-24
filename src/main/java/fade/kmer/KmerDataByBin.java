package fade.kmer;

import fade.util.Data;

import java.util.Hashtable;

public class KmerDataByBin implements Data {
    Hashtable<Kmer, Long> kmer_freq_map;

    public KmerDataByBin(Hashtable<Kmer, Long> kmer_freq_map) {
        this.kmer_freq_map = kmer_freq_map;
    }

    @Override
    public long size() {
        return kmer_freq_map.size();
    }
}
