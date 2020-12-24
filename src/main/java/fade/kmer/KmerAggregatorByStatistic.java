package fade.kmer;

import fade.*;
import fade.util.CountValue;
import fade.util.Value;
import fade.util.Data;
import fade.util.Statistic;
import fade.util.SequenceId;
import fade.util.Configuration;
import scala.Tuple2;

import java.util.*;

public class KmerAggregatorByStatistic extends StatisticAggregatorByStatistic {
    public KmerAggregatorByStatistic(Configuration conf) {
        super(conf);
    }

    @Override
    public List<Tuple2<Statistic, Tuple2<SequenceId, Value>>> call(Tuple2<Statistic, Iterable<Tuple2<SequenceId, Data>>> seq_feature_data) {
        Iterator<Tuple2<SequenceId, Data>> iter = seq_feature_data._2.iterator();
        int n = getConf().getInt("n");

        long[] frequencies = new long[n];
        Statistic kmer = seq_feature_data._1;
        Tuple2<SequenceId, Data> seq_data;

        while (iter.hasNext()) {
            seq_data = iter.next();
            frequencies[seq_data._1.id] += ((KmerDataByFeature)seq_data._2).count;
        }

        List<Tuple2<Statistic, Tuple2<SequenceId, Value>>> result = new ArrayList<>();

        for (int id = 0; id < n; id++)
            result.add(new Tuple2<>(kmer, new Tuple2<>(new SequenceId(id), new CountValue(frequencies[id]))));

        return result;
    }
}
