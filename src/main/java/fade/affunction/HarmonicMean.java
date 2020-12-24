package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.util.*;

public class HarmonicMean extends AFFunctionEvaluatorByStatistic implements Similarity {
    public HarmonicMean(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        double count1 = ((CountValue)s1).count;
        double count2 = ((CountValue)s2).count;

        // pseudocount
        count1 += count1 == 0 ? 1 : 0;
        count2 += count2 == 0 ? 1 : 0;

        return new AFValue((count1 * count2) / (count1 + count2));
    }

    @Override
    public AFValue combinePartialAFValues(AFValue d1, AFValue d2) {
        return new AFValue(d1.value + d2.value);
    }

    @Override
    public AFValue finalizeAFValue(AFValue d) {
        return new AFValue(2 * d.value);
    }
}