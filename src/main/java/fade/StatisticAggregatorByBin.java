package fade;

import fade.util.Value;
import fade.util.BinId;
import fade.util.Data;
import fade.util.Statistic;
import fade.util.SequenceId;
import fade.util.Configuration;
import scala.Tuple2;

import java.util.Collection;

public abstract class StatisticAggregatorByBin extends StatisticAggregator<Tuple2<BinId, Iterable<Tuple2<SequenceId, Data>>>, Collection<Tuple2<Statistic, Tuple2<SequenceId, Value>>>> {
    public StatisticAggregatorByBin(Configuration conf) {
        super(conf);
    }
}
