package fade.simulation;

import fade.util.Chunk;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.random.RandomRDDs;
import scala.Tuple2;

import java.util.*;


class Randomizer {

	static JavaRDD<Chunk> randomize(JavaRDD<Chunk> records, int rnd_split_len, int n_sequences, int k, JavaSparkContext jsc) {
		long n_records = records.count();

		if (rnd_split_len > 1) {

			JavaPairRDD<Long, byte[]> id_chunks = records.flatMap(record -> {
				int len = record.data.length;

				ArrayList<byte[]> chunks = new ArrayList<>(len / rnd_split_len + 1);


				for (int off = 0; off < len; off += rnd_split_len)
					chunks.add(Arrays.copyOfRange(record.data, off, Math.min(off + rnd_split_len, len)));

				return chunks.iterator();
			}).zipWithIndex().mapToPair(chunk_id -> new Tuple2<>(chunk_id._2, chunk_id._1));

			long n_chunks = id_chunks.count();

			JavaPairRDD<Long, Long> id_randoms = RandomRDDs.uniformJavaRDD(jsc, n_chunks, records.getNumPartitions())
					.zipWithIndex().mapToPair(rnds_id -> new Tuple2<>(rnds_id._2, (long) (rnds_id._1 * n_records)));

			return id_chunks.cogroup(id_randoms).mapToPair(id_chunks_randoms -> {
				// mapping 1-1, exactly one value
				byte[] chunk = id_chunks_randoms._2._1.iterator().next();
				long rnd_record = id_chunks_randoms._2._2.iterator().next();

				return new Tuple2<>(rnd_record, chunk);
			}).reduceByKey((data1, data2) -> {
				byte[] data = new byte[data1.length + data2.length];

				System.arraycopy(data1, 0, data, 0, data1.length);
				System.arraycopy(data2, 0, data, data1.length, data2.length);

				return data;
			}).map(t -> new Chunk((int) (t._1 % n_sequences), t._1, t._2)).filter(t -> t.data.length >= k);

		}
		else { // rnd_split_len == 1
			JavaPairRDD<Long, Byte> id_chunks = records.flatMap(record -> {
				int len = record.data.length;

				ArrayList<Byte> chunks = new ArrayList<>(len / rnd_split_len + 1);


				for (int off = 0; off < len; off += 1)
					chunks.add(record.data[off]);

				return chunks.iterator();
			}).zipWithIndex().mapToPair(chunk_id -> new Tuple2<>(chunk_id._2, chunk_id._1));

			long n_chunks = id_chunks.count();

			JavaPairRDD<Long, Long> id_randoms = RandomRDDs.uniformJavaRDD(jsc, n_chunks, records.getNumPartitions())
					.zipWithIndex().mapToPair(rnds_id -> new Tuple2<>(rnds_id._2, (long) (rnds_id._1 * n_records)));

			return id_chunks.cogroup(id_randoms).mapToPair(id_chunks_randoms -> {
				// mapping 1-1, exactly one value
				byte chunk = id_chunks_randoms._2._1.iterator().next();
				long rnd_record = id_chunks_randoms._2._2.iterator().next();

				return new Tuple2<>(rnd_record, chunk);
			}).aggregateByKey(new byte[0],
					(data1, v) -> {
						byte[] data = new byte[data1.length + 1];

						System.arraycopy(data1, 0, data, 0, data1.length);
						data[data.length-1] = v;

						return data;
					},
					(data1, data2) -> {
				byte[] data = new byte[data1.length + data2.length];

				System.arraycopy(data1, 0, data, 0, data1.length);
				System.arraycopy(data2, 0, data, data1.length, data2.length);

				return data;
			}).map(t -> new Chunk((int) (t._1 % n_sequences), t._1, t._2)).filter(t -> t.data.length >= k);
		}
	}
}