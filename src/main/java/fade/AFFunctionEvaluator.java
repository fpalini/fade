package fade;

import fade.util.AFValue;
import fade.util.Value;
import fade.util.Configuration;
import fade.util.SequencePair;
import fade.util.Statistic;
import scala.Tuple2;

import java.util.List;

public abstract class AFFunctionEvaluator<T> extends FadeFunction<T, List<Tuple2<SequencePair, AFValue>>> {

    public AFFunctionEvaluator(Configuration conf) {
        super(conf);
    }

    long count1, count2;
    long pseudo1, pseudo2;
    long countA1, countA2, countC1, countC2, countG1, countG2, countT1, countT2;
    double stdev1, stdev2;
    long distinct, distinct1, distinct2;
    Statistic statistic;

    abstract public AFValue evaluatePartialAFValue(Value s1, Value s2);
    abstract public AFValue combinePartialAFValues(AFValue d1, AFValue d2);
    abstract public AFValue finalizeAFValue(AFValue d);

    public void setCount1(long count1) {
        this.count1 = count1;
    }
    public void setCount2(long count2) {
        this.count2 = count2;
    }
    public void setPseudo1(long pseudo1) {
        this.pseudo1 = pseudo1;
    }
    public void setPseudo2(long pseudo2) {
        this.pseudo2 = pseudo2;
    }
    public void setStdev1(double stdev1) { this.stdev1 = stdev1; }
    public void setStdev2(double stdev2) { this.stdev2 = stdev2; }
    public void setDistinct(long distinct) {
        this.distinct = distinct;
    }
    public void setDistinct1(Long distinct1){
        this.distinct1 = distinct1;
    }
    public void setDistinct2(Long distinct2){
        this.distinct2 = distinct2;
    }
    public void setCountA1(long countA1) {
        this.countA1 = countA1;
    }
    public void setCountA2(long countA2) {
        this.countA2 = countA2;
    }
    public void setCountC1(long countC1) {
        this.countC1 = countC1;
    }
    public void setCountC2(long countC2) {
        this.countC2 = countC2;
    }
    public void setCountG1(long countG1) {
        this.countG1 = countG1;
    }
    public void setCountG2(long countG2) {
        this.countG2 = countG2;
    }
    public void setCountT1(long countT1) {
        this.countT1 = countT1;
    }
    public void setCountT2(long countT2) {
        this.countT2 = countT2;
    }
    public void setStatistic(Statistic statistic) {
        this.statistic = statistic;
    }

    public long getCount1() {
        return count1;
    }
    public long getCount2() {
        return count2;
    }
    public long getPseudo1() {
        return pseudo1;
    }
    public long getPseudo2() {
        return pseudo2;
    }
    public double getStdev1() {
        return stdev1;
    }
    public double getStdev2() {
        return stdev2;
    }
    public long getDistinct() {
        return distinct;
    }
    public long getDistinct1() {
        return distinct1;
    }
    public long getDistinct2() {
        return distinct2;
    }
    public long getCountA1() {
        return countA1;
    }
    public long getCountA2() {
        return countA2;
    }
    public long getCountC1() {
        return countC1;
    }
    public long getCountC2() {
        return countC2;
    }
    public long getCountG1() {
        return countG1;
    }
    public long getCountG2() {
        return countG2;
    }
    public long getCountT1() {
        return countT1;
    }
    public long getCountT2() {
        return countT2;
    }
    public Statistic getStatistic() {
        return statistic;
    }
}