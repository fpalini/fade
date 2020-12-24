package fade;

import fade.util.Value;
import fade.util.*;
import scala.Tuple2;

import java.util.Collection;

public abstract class StatisticAggregatorByBinToSeqPair extends StatisticAggregator<Tuple2<BinId, Iterable<Tuple2<SequenceId, Data>>>, Collection<Tuple2<SequencePair, Value>>> {
    public StatisticAggregatorByBinToSeqPair(Configuration conf) {
        super(conf);
    }
}
