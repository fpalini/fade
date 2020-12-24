package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.util.*;

public class Jaccard extends AFFunctionEvaluatorByStatistic implements Similarity {
    public Jaccard(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        long count1 = ((CountValue)s1).count;
        long count2 = ((CountValue)s2).count;

        return count1 > 0 && count2 > 0 ? new AFValue(1) : new AFValue(0);
    }

    @Override
    public AFValue combinePartialAFValues(AFValue d1, AFValue d2) {
        return new AFValue(d1.value + d2.value);
    }

    @Override
    public AFValue finalizeAFValue(AFValue intersection) {
        double union = getDistinct1() + getDistinct2() - intersection.value;
        return new AFValue(intersection.value / union);
    }
}