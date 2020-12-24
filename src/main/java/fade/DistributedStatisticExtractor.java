package fade;

import fade.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

class DistributedStatisticExtractor {

	static JavaPairRDD<BinId, Tuple2<SequenceId, Data>> extractByBin(JavaRDD<Chunk> input, StatisticExtractorByBin extractor) {
		return input.flatMapToPair(item -> extractor.call(item).iterator());
	}

	static JavaPairRDD<Statistic, Tuple2<SequenceId, Data>> extractByStatistic(JavaRDD<Chunk> input, StatisticExtractorByStatistic extractor) {
		return input.flatMapToPair(item -> extractor.call(item).iterator());
	}

	static JavaPairRDD<UniqueId, Tuple2<SequenceId, Data>> extractByUnique(JavaRDD<Chunk> input, StatisticExtractorByUnique extractor) {
		return input.flatMapToPair(item -> extractor.call(item).iterator());
	}
}
