package fade.mash;

import fade.StatisticAggregatorByBin;
import fade.util.*;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static fade.mash.MashAggregator.merge;

public class MashAggregatorJ extends StatisticAggregatorByBin {

	public MashAggregatorJ(Configuration conf) {
		super(conf);
	}

	public Collection<Tuple2<Statistic, Tuple2<SequenceId, Value>>> call(Tuple2<BinId, Iterable<Tuple2<SequenceId, Data>>> t) {

		int s = getConf().getInt("s");

		long[] sketches = null;
		List<Tuple2<Statistic, Tuple2<SequenceId, Value>>> output = new ArrayList<>(s);

		for (Tuple2<SequenceId, Data> tu : t._2) {
			long[] sketches1 = ((MashData)tu._2).sketches;

			sketches = sketches == null? sketches1 : merge(sketches, sketches1, s);
		}

		for(long l : sketches)
			output.add(new Tuple2<>(new HashCode(l), new Tuple2<>(new SequenceId(t._1.id), new CountValue(1))));

		return output;
	}
}

	
	





