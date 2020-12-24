package fade;

import fade.util.Value;
import fade.util.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Collection;

public abstract class StatisticAggregatorByUnique extends StatisticAggregator<Collection<Tuple2<UniqueId, Tuple2<SequenceId, Data>>>, Collection<Tuple2<UniqueId, Tuple3<SequenceId, Statistic, Value>>>> {
    public StatisticAggregatorByUnique(Configuration conf) {
        super(conf);
    }
}
