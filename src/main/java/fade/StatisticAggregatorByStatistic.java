package fade;

import fade.util.Value;
import fade.util.*;
import scala.Tuple2;

import java.util.Collection;

public abstract class StatisticAggregatorByStatistic extends StatisticAggregator<Tuple2<Statistic, Iterable<Tuple2<SequenceId, Data>>>, Collection<Tuple2<Statistic, Tuple2<SequenceId, Value>>>> {
    public StatisticAggregatorByStatistic(Configuration conf) {
        super(conf);
    }
}
