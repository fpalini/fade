package fade.util;

import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

public class Reduce1mers implements Function2<long[], long[], long[]> {
    @Override
    public long[] call(long[] v1, long[] v2) throws Exception {
        long[] result = new long[4];
        Arrays.setAll(result, i -> v1[i] + v2[i]);

        return result;
    }
}
