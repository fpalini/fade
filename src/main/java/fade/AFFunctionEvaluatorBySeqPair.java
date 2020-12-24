package fade;

import fade.util.AFValue;
import fade.util.Value;
import fade.exception.OperationNotImplemented;
import fade.util.SequencePair;
import fade.util.Configuration;
import scala.Tuple2;

public abstract class AFFunctionEvaluatorBySeqPair extends AFFunctionEvaluator<Tuple2<SequencePair, Iterable<Value>>> {
    public AFFunctionEvaluatorBySeqPair(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        throw new OperationNotImplemented();
    }

    @Override
    public AFValue combinePartialAFValues(AFValue p1, AFValue p2) {
        throw new OperationNotImplemented();
    }
}
