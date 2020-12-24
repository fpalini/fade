package fade;

import fade.util.Value;
import fade.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

public class DistributedAggregatedStatisticFilter {

    static JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> filterByBin(JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> input, String regex) {
        return regex == null? input : input.filter(item -> item._1.filter(regex));
    }

    static JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> filterByStatistic(JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> input, String regex) {
        return regex == null? input : input.filter(item -> item._1.filter(regex));
    }

    public static JavaPairRDD<UniqueId, Tuple3<SequenceId, Statistic, Value>> filterByUnique(JavaPairRDD<UniqueId, Tuple3<SequenceId, Statistic, Value>> input, String regex) {
        return regex == null? input : input.filter(item -> item._2._2().filter(regex));
    }

    static JavaPairRDD<SequencePair, Value> filterByBinToSeqPair(JavaPairRDD<SequencePair, Value> input, String regex) {
        return input;
    }
}
