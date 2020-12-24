package fade.kmer;

import fade.StatisticAggregatorByUnique;
import fade.util.CountValue;
import fade.util.Value;
import fade.util.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class KmerAggregatorByUnique extends StatisticAggregatorByUnique {
    public KmerAggregatorByUnique(Configuration conf) {
        super(conf);
    }

    @Override
    public Collection<Tuple2<UniqueId, Tuple3<SequenceId, Statistic, Value>>> call(Collection<Tuple2<UniqueId, Tuple2<SequenceId, Data>>> id_seq_stat_val) {
        Iterator<Tuple2<UniqueId, Tuple2<SequenceId, Data>>> iter = id_seq_stat_val.iterator();
        int n = getConf().getInt("n");

        HashMap<Kmer, long[]> frequencies_map = new HashMap<>();
        long[] frequencies;
        Tuple2<SequenceId, Data> item;
        KmerDataByUnique kmer_freq;

        while (iter.hasNext()) {
            item = iter.next()._2;
            kmer_freq = ((KmerDataByUnique)item._2);
            frequencies = frequencies_map.computeIfAbsent(kmer_freq.kmer, k -> new long[n]);
            frequencies[item._1.id] += kmer_freq.count;
        }

        List<Tuple2<UniqueId, Tuple3<SequenceId, Statistic, Value>>> result = new ArrayList<>();
        Kmer kmer;

        for (Map.Entry<Kmer, long[]> kv : frequencies_map.entrySet()) {
            kmer = kv.getKey();
            frequencies = kv.getValue();
            for (int id = 0; id < n; id++)
                result.add(new Tuple2<>(UniqueId.getInstance(), new Tuple3<>(new SequenceId(id), kmer, new CountValue(frequencies[id]))));
        }

        return result;
    }
}
