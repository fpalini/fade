package fade.util;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class StDevByStatistic implements PairFunction<Tuple2<Statistic, Tuple2<SequenceId, Value>>, SequenceId, Double> {
    private Configuration conf;

    public StDevByStatistic(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Tuple2<SequenceId, Double> call(Tuple2<Statistic, Tuple2<SequenceId, Value>> kmer_id_count) throws Exception {
        long kmers = conf.getLong("distinct");
        int k = conf.getInt("k");
        long total = conf.getLong("len_s" + kmer_id_count._2._1.id) - k+1;
        double mean = 1.0 * total / kmers;
        long count = ((CountValue) kmer_id_count._2._2).count;

        return new Tuple2<>(kmer_id_count._2._1, Math.pow(count - mean, 2));
    }
}
