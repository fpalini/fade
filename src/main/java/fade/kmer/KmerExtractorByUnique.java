package fade.kmer;

import fade.StatisticExtractorByUnique;
import fade.util.*;
import scala.Tuple2;

import java.util.*;

public class KmerExtractorByUnique extends StatisticExtractorByUnique {

    public KmerExtractorByUnique(Configuration conf) {
        super(conf);
    }

    @Override
    public Collection<Tuple2<UniqueId, Tuple2<SequenceId, Data>>> call(Chunk chunk) {
        int k = getConf().getInt("k");
        boolean canonical = getConf().getBoolean("canonical", Defaults.CANONICAL);

        SequenceId seqId = new SequenceId(chunk.id);
        int len = chunk.data.length;

        List<Tuple2<UniqueId, Tuple2<SequenceId, Data>>> kmers = new ArrayList<>();

        Hashtable<Kmer, Long> frequencies = new Hashtable<>();
        KmerTool tool = new KmerTool(chunk.data, k);
        Kmer kmer;

        for (int i = 0; i < len - (k-1); i++) {
            kmer = new Kmer(canonical? tool.nextKmerCan() : tool.nextKmer(), k);
            frequencies.merge(kmer, 1L, Long::sum);
        }

        for (Map.Entry<Kmer, Long> kmer_freq : frequencies.entrySet())
            kmers.add(new Tuple2<>(UniqueId.getInstance(), new Tuple2<>(seqId, new KmerDataByUnique(kmer_freq.getKey(), kmer_freq.getValue()))));

        return kmers;
    }
}
