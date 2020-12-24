package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.util.*;

public class D2 extends AFFunctionEvaluatorByStatistic implements Similarity {
    public D2(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        double count1 = ((CountValue)s1).count;
        double count2 = ((CountValue)s2).count;

        return new AFValue(count1 * count2);
    }

    @Override
    public AFValue combinePartialAFValues(AFValue d1, AFValue d2) {
        return new AFValue(d1.value + d2.value);
    }

    @Override
    public AFValue finalizeAFValue(AFValue d) {
        return d;
    }
}
