package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.util.*;

public class Kulczynski2 extends AFFunctionEvaluatorByStatistic implements Similarity {
    public Kulczynski2(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        double count1 = ((CountValue)s1).count;
        double count2 = ((CountValue)s2).count;

        return new AFValue(Math.min(count1, count2));
    }

    @Override
    public AFValue combinePartialAFValues(AFValue d1, AFValue d2) {
        return new AFValue(d1.value + d2.value);
    }

    @Override
    public AFValue finalizeAFValue(AFValue d) {
        int k = getConf().getInt("k");

        double mu1 = 1.0 * getCount1() / getDistinct();
        double mu2 = 1.0 * getCount2() / getDistinct();

        double A = Math.pow(4, k) * (mu1 + mu2) / (2 * mu1 * mu2);

        return new AFValue(A * d.value);
    }
}
