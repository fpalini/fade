package fade.util;

import fade.util.Configuration;
import org.apache.spark.api.java.function.Function;

public class StDevFinalize implements Function<Double, Double> {
    private Configuration conf;

    public StDevFinalize(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Double call(Double sqr_sum) throws Exception {
        return Math.sqrt(sqr_sum / conf.getLong("distinct"));
    }
}
