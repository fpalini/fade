package fade;


import fade.util.Configuration;

abstract class StatisticAggregator<T, R> extends FadeFunction<T, R> {
    public StatisticAggregator(Configuration conf) {
        super(conf);
    }
}
