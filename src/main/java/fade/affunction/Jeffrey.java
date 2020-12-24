package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.util.AFValue;
import fade.util.CountValue;
import fade.util.Value;
import fade.util.Configuration;

public class Jeffrey extends AFFunctionEvaluatorByStatistic {
    public Jeffrey(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        double count1 = ((CountValue)s1).count;
        double count2 = ((CountValue)s2).count;

        // pseudocount
        count1 += count1 == 0 ? 1 : 0;
        count2 += count2 == 0 ? 1 : 0;

        double freq1 = count1 / (getCount1() + getPseudo1());
        double freq2 = count2 / (getCount2() + getPseudo2());

        return new AFValue((freq1 - freq2) * Math.log(freq1 / freq2));
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
