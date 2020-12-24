package fade;

import fade.util.BinId;
import fade.util.Data;
import fade.util.Configuration;
import fade.util.SequenceId;
import scala.Tuple2;

import java.util.Collection;
import java.util.List;

public abstract class StatisticExtractorByBin extends StatisticExtractor<Collection<Tuple2<BinId, Tuple2<SequenceId, Data>>>> {
    public StatisticExtractorByBin(Configuration conf) {
        super(conf);
    }
}
