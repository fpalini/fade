package fade.kmer.fast;

import fade.*;
import fade.util.*;
import scala.Tuple2;

import java.util.Collection;

public class FastKmerAggregatorByBin extends StatisticAggregatorByBin {
    public FastKmerAggregatorByBin(Configuration conf) {
        super(conf);
    }

    @Override
    public Collection<Tuple2<Statistic, Tuple2<SequenceId, Value>>> call(Tuple2<BinId, Iterable<Tuple2<SequenceId, Data>>> binId_dataList) {

        int k = getConf().getInt("k");
        int x = getConf().getInt("x", 3);
        int n = getConf().getInt("n");
        boolean canonical = getConf().getBoolean("canonical", Defaults.CANONICAL);

        return FastKmerUtils.extractKXmersAndCount(k, x, n, canonical, binId_dataList);
    }
}
