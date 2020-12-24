package fade;

import fade.util.Data;
import fade.util.Statistic;
import fade.util.SequenceId;
import fade.util.Configuration;
import scala.Tuple2;

import java.util.Collection;
import java.util.List;

public abstract class StatisticExtractorByStatistic extends StatisticExtractor<Collection<Tuple2<Statistic, Tuple2<SequenceId, Data>>>> {
    public StatisticExtractorByStatistic(Configuration conf) {
        super(conf);
    }
}
