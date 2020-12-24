package fade;

import fade.util.AFValue;
import fade.util.Chunk;
import fade.util.SequencePair;
import fastdoop.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public class IOManager implements Serializable {
    public Broadcast<List<String>> id2key_bc;
    private Broadcast<HashMap<String, Integer>> key2id_bc;
    public JavaRDD<Chunk> chunks_rdd;
    private boolean assembled;

    public void readAssembledLong(String input, int k, JavaSparkContext spark) {
        assembled = true;

        spark.hadoopConfiguration().setInt("k", k);

        JavaRDD<Record> fastdoop_rdd = spark.newAPIHadoopFile(input, FASTAlongInputFileFormat.class,
                Text.class, PartialSequence.class, spark.hadoopConfiguration()).map(t -> t._2);

        simplifyByFileName(fastdoop_rdd, spark, k, '.');
    }

    public void readAssembledShort(String input, int buffer_size, JavaSparkContext spark) {
        assembled = true;

        spark.hadoopConfiguration().setInt("look_ahead_buffer_size", buffer_size);

        JavaRDD<Record> fastdoop_rdd = spark.newAPIHadoopFile(input, FASTAshortInputFileFormat.class,
                Text.class, Record.class, spark.hadoopConfiguration()).map(t -> t._2);

        simplifyBySequenceKey(fastdoop_rdd, spark, " ");
    }

    public void readUnassembledFasta(String input, int buffer_size, int k, JavaSparkContext spark) {
        assembled = false;

        spark.hadoopConfiguration().setInt("look_ahead_buffer_size", buffer_size);

        JavaRDD<Record> fastdoop_rdd = spark.newAPIHadoopFile(input, FASTAshortInputFileFormat.class,
                Text.class, Record.class, spark.hadoopConfiguration()).map(t -> t._2);

        simplifyByFileName(fastdoop_rdd, spark, k, '.');
        // joinReverseReads(k, spark);
    }

    public void readUnassembledFastq(String input, int buffer_size, int k, JavaSparkContext spark) {
        assembled = false;

        spark.hadoopConfiguration().setInt("look_ahead_buffer_size", buffer_size);

        JavaRDD<Record> fastdoop_rdd = spark.newAPIHadoopFile(input, FASTQInputFileFormat.class,
                Text.class, QRecord.class, spark.hadoopConfiguration()).map(t -> t._2);

        simplifyByFileName(fastdoop_rdd, spark, k, '.');
        // joinReverseReads(k, spark);
    }

    public void split(int chunk_len, int k) {
        chunks_rdd = IOManager.split(chunks_rdd, assembled, chunk_len, k);
    }

    public static JavaRDD<Chunk> split(JavaRDD<Chunk> sequences, boolean assembled, int chunk_len, int k) {
        JavaRDD<Chunk> sequences_split = sequences;

        if (assembled) {
            sequences = sequences.filter(sequence -> sequence.data.length >= k); // TODO: output a warning

            sequences_split = sequences.flatMap(sequence -> {
                List<Chunk> sequence_split = new ArrayList<>();

                int len = sequence.data.length;
                int chunk_offset = chunk_len - (k - 1);

                for (int off = 0; off < len - (k - 1); off += chunk_offset)
                    sequence_split.add(new Chunk(sequence.id, sequence.offset + off, Arrays.copyOfRange(sequence.data, off, Math.min(off + chunk_len, len))));

                return sequence_split.iterator();
            });
        }

        return sequences_split;
    }

    private void simplifyByFileName(JavaRDD<Record> records, JavaSparkContext spark, int k, char nameSeparator) {
        List<String> id2key = records.map(record -> {
            String name = record.getFileName();
            if (name.indexOf(nameSeparator) != -1)
                return name.substring(0, name.lastIndexOf(nameSeparator));
            else
                return name;
        }).distinct().collect();

        id2key = new ArrayList<>(id2key);
        id2key.sort(String::compareToIgnoreCase);
        id2key_bc = spark.broadcast(id2key);

        HashMap<String, Integer> key2id = new HashMap<>();

        for (int i = 0; i < id2key.size(); i++)
            key2id.put(id2key.get(i), i);

        key2id_bc = spark.broadcast(key2id);

        chunks_rdd = records.map(record -> {
            byte[] bytes = record.getValue().toUpperCase().replaceAll("[^ACGT]+", "").getBytes();
            String name = record.getFileName();
            if (name.indexOf(nameSeparator) != -1)
                name = name.substring(0, name.lastIndexOf(nameSeparator));

            return new Chunk(key2id_bc.getValue().get(name), record.getFileOffset(), bytes);
        }).filter(chunk -> chunk.data.length >= k);
    }

    private void simplifyBySequenceKey(JavaRDD<Record> records, JavaSparkContext spark, String keySeparator) {
        List<String> id2key = records.map(record -> record.getKey().split(keySeparator)[0].trim()).distinct().collect();

        id2key = new ArrayList<>(id2key);
        id2key.sort(String::compareToIgnoreCase);
        id2key_bc = spark.broadcast(id2key);

        HashMap<String, Integer> key2id = new HashMap<>();

        for (int i = 0; i < id2key.size(); i++)
            key2id.put(id2key.get(i), i);

        key2id_bc = spark.broadcast(key2id);

        chunks_rdd = records.map(record -> {
            byte[] bytes = record.getValue().toUpperCase().replaceAll("[^ACGT]+", "").getBytes();

            return new Chunk(key2id_bc.getValue().get(record.getKey().split(keySeparator)[0].trim()), record.getFileOffset(), bytes);
        });
    }

    public JavaPairRDD<Tuple2<String, String>, AFValue[]> addLabels(JavaPairRDD<SequencePair, AFValue[]> distance_matrix) {
        return distance_matrix.mapToPair(t -> new Tuple2<>(new Tuple2<>(id2key_bc.getValue().get(t._1.id1), id2key_bc.getValue().get(t._1.id2)), t._2));
    }
}
