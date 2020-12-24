package fade.kmer;

import fade.StatisticExtractorByStatistic;
import fade.util.*;
import scala.Tuple2;

import java.util.*;

public class KmerExtractorByStatistic extends StatisticExtractorByStatistic {

    public KmerExtractorByStatistic(Configuration conf) {
        super(conf);
    }

    @Override
    public List<Tuple2<Statistic, Tuple2<SequenceId, Data>>> call(Chunk chunk) {
        int k = getConf().getInt("k");
        boolean canonical = getConf().getBoolean("canonical", Defaults.CANONICAL);

        SequenceId seqId = new SequenceId(chunk.id);
        int len = chunk.data.length;

        List<Tuple2<Statistic, Tuple2<SequenceId, Data>>> kmers = new ArrayList<>();

        Hashtable<Kmer, Long> frequencies = new Hashtable<>();
        KmerTool tool = new KmerTool(chunk.data, k);
        Kmer kmer;

        for (int i = 0; i < len - (k-1); i++) {
            kmer = new Kmer(canonical? tool.nextKmerCan() : tool.nextKmer(), k);
            frequencies.merge(kmer, 1L, Long::sum);
        }

        for (Map.Entry<Kmer, Long> kmer_freq : frequencies.entrySet())
            kmers.add(new Tuple2<>(kmer_freq.getKey(), new Tuple2<>(seqId, new KmerDataByFeature(kmer_freq.getValue()))));

        return kmers;
    }
}
