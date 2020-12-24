package fade.kmer;

import fade.StatisticAggregatorByBin;
import fade.util.CountValue;
import fade.util.Value;
import fade.util.*;
import scala.Tuple2;

import java.util.*;

public class KmerAggregatorByBin extends StatisticAggregatorByBin {
    public KmerAggregatorByBin(Configuration conf) {
        super(conf);
    }

    @Override
    public Collection<Tuple2<Statistic, Tuple2<SequenceId, Value>>> call(Tuple2<BinId, Iterable<Tuple2<SequenceId, Data>>> bin_seq_data) {
        Iterator<Tuple2<SequenceId, Data>> iter = bin_seq_data._2.iterator();
        int n = getConf().getInt("n");

        HashMap<Kmer, long[]> frequencies_map = new HashMap<>();
        long[] frequencies;
        Tuple2<SequenceId, Data> item;
        Hashtable<Kmer, Long> hashtable;

        while (iter.hasNext()) {
            item = iter.next();
            hashtable = ((KmerDataByBin)item._2).kmer_freq_map;

            for (Map.Entry<Kmer, Long> kmer_freq : hashtable.entrySet()) {
                frequencies = frequencies_map.computeIfAbsent(kmer_freq.getKey(), k -> new long[n]);
                frequencies[item._1.id] += kmer_freq.getValue();
            }
        }

        List<Tuple2<Statistic, Tuple2<SequenceId, Value>>> result = new ArrayList<>();
        Kmer kmer;

        for (Map.Entry<Kmer, long[]> kv : frequencies_map.entrySet()) {
            kmer = kv.getKey();
            frequencies = kv.getValue();
            for (int id = 0; id < n; id++)
                result.add(new Tuple2<>(kmer, new Tuple2<>(new SequenceId(id), new CountValue(frequencies[id]))));
        }

        return result;
    }
}
