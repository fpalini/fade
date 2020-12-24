package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.mash.MashAggregator;
import fade.mash.MashValue;
import fade.util.AFValue;
import fade.util.Configuration;
import fade.util.Value;

import java.util.Arrays;

public class Mash extends AFFunctionEvaluatorByStatistic {

    public Mash(Configuration conf) {
        super(conf);
    }

    public AFValue evaluatePartialAFValue(Value v1, Value v2) {
        int s = getConf().getInt("s");

        long[] A = ((MashValue)v1).hashes;
        long[] B = ((MashValue)v2).hashes;



        return new AFValue(merge_jaccard(A, B, s));
    }

    public AFValue combinePartialAFValues(AFValue d1, AFValue d2) {
        return new AFValue(d1.value + d2.value);
    }

    public AFValue finalizeAFValue(AFValue d) {
        int k = getConf().getInt("k");

        double j = d.value;

        double distance = j == 0? 1 : (-1.0 / k) * (Math.log(2 * j/(1 + j)));

        return new AFValue(distance);

    }

    public static double merge_jaccard(long[] a, long[] b, int s) {
        long[] c = new long[s];
        double intersect = 0;
        int i = 0, j = 0, k = 0;

        while (k < s && i < a.length && j < b.length) {
            c[k++] = Long.compareUnsigned(a[i], b[j]) < 1 ? a[i++] : b[j++];

            if (i < a.length && c[k-1] == a[i]) {
                intersect++;
                i++;
            }

            if (j < b.length && c[k-1] == b[j]) {
                intersect++;
                j++;
            }
        }

        while (k < s && i < a.length) {
            c[k++] = a[i++];

            if (i < a.length && c[k-1] == a[i]) {
                intersect++;
                i++;
            }
        }

        while (k < s && j < b.length) {
            c[k++] = b[j++];

            if (j < b.length && c[k-1] == b[j]) {
                intersect++;
                j++;
            }
        }

        return intersect / Math.min(Math.max(a.length, b.length), s);
    }
}