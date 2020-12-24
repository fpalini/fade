package fade.kmer.fast;

import fade.util.*;
import fade.StatisticExtractorByBin;
import scala.Tuple2;

import java.util.List;

public class FastKmerExtractorByBin extends StatisticExtractorByBin {

    public FastKmerExtractorByBin(Configuration conf) {
        super(conf);
    }

    @Override
    public List<Tuple2<BinId, Tuple2<SequenceId, Data>>> call(Chunk chunk) {
        int k = getConf().getInt("k");
        int m = getConf().getInt("m", k/2);
        int b = getConf().getInt("slices", Defaults.SLICES);

        return FastKmerUtils.getSuperKmers(k, m, b, chunk);
    }
}
