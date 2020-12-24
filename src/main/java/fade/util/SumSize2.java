package fade.util;

import org.apache.spark.api.java.function.Function2;

public class SumSize2 implements Function2<Long, Long, Long> {
    @Override
    public Long call(Long v1, Long v2) {
        return v1 + v2;
    }
}
