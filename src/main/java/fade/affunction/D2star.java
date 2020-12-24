package fade.affunction;

import fade.AFFunctionEvaluatorByStatistic;
import fade.kmer.fast.FastKmer;
import fade.kmer.Kmer;
import fade.util.*;

public class D2star extends AFFunctionEvaluatorByStatistic implements Similarity {
    public D2star(Configuration conf) {
        super(conf);
    }

    @Override
    public AFValue evaluatePartialAFValue(Value s1, Value s2) {
        double count1 = ((CountValue)s1).count;
        double count2 = ((CountValue)s2).count;

        double pA1 = getCountA1();
        double pC1 = getCountC1();
        double pG1 = getCountG1();
        double pT1 = getCountT1();

        double len1 = pA1 + pC1 + pG1 + pT1;

        pA1 = pA1 / len1;
        pC1 = pC1 / len1;
        pG1 = pG1 / len1;
        pT1 = pT1 / len1;

        double pA2 = getCountA2();
        double pC2 = getCountC2();
        double pG2 = getCountG2();
        double pT2 = getCountT2();

        double len2 = pA2 + pC2 + pG2 + pT2;

        pA2 = pA2 / len2;
        pC2 = pC2 / len2;
        pG2 = pG2 / len2;
        pT2 = pT2 / len2;

        double pw1 = 1;
        double pw2 = 1;

        Statistic statistic = getStatistic();
        byte[] kmer;

        if (statistic instanceof FastKmer)
            kmer = ((FastKmer)statistic).kmer.toByteArray();
        else {// statistic instanceof Kmer
            Kmer kmer_stat = ((Kmer) statistic);
            kmer = KmerTool.long2bytes(kmer_stat.kmer, kmer_stat.k);
        }

        for (byte nucleodite : kmer)
            switch (nucleodite) {
                case 'A':
                    pw1 *= pA1;
                    pw2 *= pA2;
                    break;
                case 'C':
                    pw1 *= pC1;
                    pw2 *= pC2;
                    break;
                case 'G':
                    pw1 *= pG1;
                    pw2 *= pG2;
                    break;
                case 'T':
                    pw1 *= pT1;
                    pw2 *= pT2;
                    break;
            }

        // pseudocount
        count1 += count1 == 0 ? 1 : 0;
        count2 += count2 == 0 ? 1 : 0;

        double count1_mod = count1 - getCount1() * pw1;
        double count2_mod = count2 - getCount2() * pw2;

        return new AFValue((count1_mod * count2_mod) / Math.sqrt(getCount1() * pw1 * getCount2() * pw2));
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
