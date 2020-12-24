package fade;

import fade.util.Configuration;
import org.apache.spark.api.java.function.Function;

public abstract class FadeFunction<T, R>  implements Function<T, R> {
    private final Configuration conf;

    public FadeFunction(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }
}
