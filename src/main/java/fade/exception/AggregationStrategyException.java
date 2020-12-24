package fade.exception;

public class AggregationStrategyException extends Exception {
    public AggregationStrategyException(String strategy) {
        super("Classes not compatible with " + strategy + " aggregation strategy.");
    }
}
