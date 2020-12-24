package fade;

import fade.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class DistributedStatisticFilter {

    static JavaPairRDD<BinId, Tuple2<SequenceId, Data>> filterByBin(JavaPairRDD<BinId, Tuple2<SequenceId, Data>> input, String regex) {
        return regex == null? input : input.filter(item -> item._2._2.filter(regex));
    }

    static JavaPairRDD<Statistic, Tuple2<SequenceId, Data>> filterByStatistic(JavaPairRDD<Statistic, Tuple2<SequenceId, Data>> input, String regex) {
        return regex == null? input : input.filter(item -> item._1.filter(regex));
    }

    public static JavaPairRDD<UniqueId, Tuple2<SequenceId, Data>> filterByUnique(JavaPairRDD<UniqueId, Tuple2<SequenceId, Data>> input, String regex) {
        return regex == null? input : input.filter(item -> item._2._2.filter(regex));
    }
}
