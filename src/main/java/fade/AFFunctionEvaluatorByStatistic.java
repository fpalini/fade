package fade;

import fade.util.AFValue;
import fade.util.Value;
import fade.util.Statistic;
import fade.util.SequenceId;
import fade.util.SequencePair;
import fade.util.Configuration;
import scala.Tuple2;

import java.util.List;

public abstract class AFFunctionEvaluatorByStatistic extends AFFunctionEvaluator<Tuple2<Statistic, Iterable<Tuple2<SequenceId, Value>>>> {

    public AFFunctionEvaluatorByStatistic(Configuration conf) {
        super(conf);
    }

    @Override
    public List<Tuple2<SequencePair, AFValue>> call(Tuple2<Statistic, Iterable<Tuple2<SequenceId, Value>>> v1) {
        return null;
    }
}
