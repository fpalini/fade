package fade.affunction;

import fade.*;
import fade.sw.SwValue;
import fade.util.AFValue;
import fade.util.Value;
import fade.util.SequencePair;
import fade.util.Configuration;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FSWM extends AFFunctionEvaluatorBySeqPair {

    public FSWM(Configuration conf) {
        super(conf);
    }

    @Override
    public List<Tuple2<SequencePair, AFValue>> call(Tuple2<SequencePair, Iterable<Value>> seqIds_stats) {
        double sw_mismatches = 0;
        double sw_dontcares = 0;

        Iterator<Value> statistics = seqIds_stats._2.iterator();
        SwValue statistic;

        while(statistics.hasNext()) {
            statistic = (SwValue) statistics.next();
            sw_mismatches += statistic.mismatches;
            sw_dontcares += statistic.dontcares;
        }

        List<Tuple2<SequencePair, AFValue>> result = new ArrayList<>();
        result.add(new Tuple2<>(seqIds_stats._1, new AFValue(sw_mismatches / sw_dontcares)));

        return result;
    }

    @Override
    public AFValue finalizeAFValue(AFValue d) {
        return new AFValue(-3.0/4 * Math.log(1 - 4.0/3 * d.value));
    }
}
