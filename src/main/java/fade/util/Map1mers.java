package fade.util;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Map1mers implements PairFunction<Chunk, Integer, long[]> {

    private final int off;

    public Map1mers(int off) {
        this.off = off;
    }

    @Override
    public Tuple2<Integer, long[]> call(Chunk chunk) throws Exception {
        long[] counts = new long[4];

        for (int i = 0; i < chunk.data.length - off; i++) {
            switch (chunk.data[i]) {
                case 'A':
                    counts[0]++;
                    break;
                case 'C':
                    counts[1]++;
                    break;
                case 'G':
                    counts[2]++;
                    break;
                case 'T':
                    counts[3]++;
                    break;
            }
        }
        return new Tuple2<>(chunk.id, counts);
    }
}
