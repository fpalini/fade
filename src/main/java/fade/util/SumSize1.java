package fade.util;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class SumSize1 implements Function2<Long, Tuple2<SequenceId, Data>, Long> {
    @Override
    public Long call(Long v1, Tuple2<SequenceId, Data> v2) {
        return v1 + v2._2.size();
    }
}
