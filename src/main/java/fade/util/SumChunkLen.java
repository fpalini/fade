package fade.util;

import org.apache.spark.api.java.function.Function2;

public class SumChunkLen implements Function2<Long, Long, Long> {
    private int off;

    public SumChunkLen(int off) {
        this.off = off;
    }

    @Override
    public Long call(Long l1, Long l2) {
        return l1 + l2 - off;
    }
}
