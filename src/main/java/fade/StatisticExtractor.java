package fade;

import fade.util.Chunk;
import fade.util.Configuration;

abstract class StatisticExtractor<R> extends FadeFunction<Chunk, R> {
    public StatisticExtractor(Configuration conf) {
        super(conf);
    }
}
