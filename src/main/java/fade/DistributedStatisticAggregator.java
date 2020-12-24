package fade;

import fade.util.Value;
import fade.util.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

class DistributedStatisticAggregator {

	static JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> aggregateByBin(JavaPairRDD<BinId, Tuple2<SequenceId, Data>> input, StatisticAggregatorByBin aggregator) {
		return input.groupByKey().flatMapToPair(item -> aggregator.call(item).iterator());
	}

	static JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> aggregateByStatistic(JavaPairRDD<Statistic, Tuple2<SequenceId, Data>> input, StatisticAggregatorByStatistic aggregator) {
		return input.groupByKey().flatMapToPair(item -> aggregator.call(item).iterator());
	}

	static JavaPairRDD<UniqueId, Tuple3<SequenceId, Statistic, Value>> aggregateByUnique(JavaPairRDD<UniqueId, Tuple2<SequenceId, Data>> input, StatisticAggregatorByUnique aggregator) {
		return input.mapPartitionsToPair(item -> aggregator.call(IteratorUtils.toList(item)).iterator());
	}

    static JavaPairRDD<SequencePair, Value> aggregateByBinToSeqPair(JavaPairRDD<BinId, Tuple2<SequenceId, Data>> input, StatisticAggregatorByBinToSeqPair aggregator) {
		return input.groupByKey().flatMapToPair(item -> aggregator.call(item).iterator());
    }
}
