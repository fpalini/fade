package fade;

import fade.util.*;
import scala.Tuple2;

import java.util.Collection;

public abstract class StatisticExtractorByUnique extends StatisticExtractor<Collection<Tuple2<UniqueId, Tuple2<SequenceId, Data>>>> {
    public StatisticExtractorByUnique(Configuration conf) {
        super(conf);
    }
}
