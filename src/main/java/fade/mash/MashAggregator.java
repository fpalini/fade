package fade.mash;

import java.util.*;

import fade.StatisticAggregatorByBin;
import fade.util.*;
import scala.Tuple2;

public class MashAggregator extends StatisticAggregatorByBin {

	public MashAggregator(Configuration conf) {
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

		output.add(new Tuple2<>(NoStatistic.getSingleton(), new Tuple2<>(new SequenceId(t._1.id), new MashValue(sketches))));

		return output;
	}

	public static long[] merge(long[] a, long[] b, int s) {
		long[] c = new long[s];
		int i = 0, j = 0, k = 0;

		while (k < s && i < a.length && j < b.length) {
			c[k++] = Long.compareUnsigned(a[i], b[j]) < 1 ? a[i++] : b[j++];

			if (i < a.length && c[k-1] == a[i])
				i++;

			if (j < b.length && c[k-1] == b[j])
				j++;
		}

		while (k < s && i < a.length) {
			c[k++] = a[i++];

			if (i < a.length && c[k-1] == a[i])
				i++;
		}

		while (k < s && j < b.length) {
			c[k++] = b[j++];

			if (j < b.length && c[k-1] == b[j])
				j++;
		}

		return Arrays.copyOf(c, k);
	}
}

	
	





