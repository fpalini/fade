package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.util.*;

public class D2Z extends AFFunctionEvaluatorByStatistic implements Similarity {
    public D2Z(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        double mean1 = 1.0 * getCount1() / getDistinct();
        double mean2 = 1.0 * getCount2() / getDistinct();

        double count1 = ((CountValue)s1).count;
        double count2 = ((CountValue)s2).count;

        count1 = (count1 - mean1) / getStdev1();
        count2 = (count2 - mean2) / getStdev2();

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
