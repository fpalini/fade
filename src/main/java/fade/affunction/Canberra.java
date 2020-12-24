package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.util.AFValue;
import fade.util.CountValue;
import fade.util.Value;
import fade.util.Configuration;

public class Canberra extends AFFunctionEvaluatorByStatistic {
    public Canberra(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        double count1 = ((CountValue)s1).count;
        double count2 = ((CountValue)s2).count;

        if (count1 + count2 == 0)
            return new AFValue(0);

        return new AFValue(Math.abs(count1 - count2) / (count1 + count2));
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
