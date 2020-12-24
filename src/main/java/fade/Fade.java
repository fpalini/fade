package fade;

import fade.util.AFValue;
import fade.exception.AggregationStrategyException;
import fade.util.CountValue;
import fade.util.Value;
import fade.util.*;
import fade.util.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Fade {
    private Configuration conf;
    private JavaSparkContext jsc;
    private StatisticExtractor extractor;
    private StatisticAggregator aggregator;
    private ArrayList<AFFunctionEvaluator> evaluators = new ArrayList<>();
    private String filterRegex, aggregatedFilterRegex;
    private IOManager ioManager;
    private JavaPairRDD<SequencePair, AFValue[]> distance_matrices;
    private String inputPath;
    private JavaRDD<Chunk> input;
    private String outputDir;
    private Strategy strategy = Defaults.STRATEGY;

    private Logger logger = Logger.getLogger(this.getClass().getName());
    

    public Fade(Configuration conf) {
        this.conf = conf;
        String master = conf.getBoolean("local", Defaults.LOCAL) ? "local[*]" : "yarn";
        java.nio.file.Path path = java.nio.file.Paths.get(conf.getString("input"));
        String appName = "FADE: " + path.getFileName().toString();

        // Define a configuration to use to interact with Spark
        SparkConf sparkConf = new SparkConf()
                .setMaster( master)
                .setAppName( appName);

        jsc = new JavaSparkContext( sparkConf);
        // conf.set("mapred.max.split.size","5000000");

        logger.info("Spark Context configurated. mapred.max.splitsize:" + conf.getInt("mapred.max.split.size"));
    }

    public void compute() throws AggregationStrategyException {
        int k = conf.getInt("k");
        boolean isAssembled = !conf.getString("assembled", Defaults.ASSEMBLED).equals("no");

        Map<Integer, Long> id_lengths = ioManager.chunks_rdd.
                mapToPair(chunk -> new Tuple2<>(chunk.id, (long) chunk.data.length)).
                reduceByKey(new SumChunkLen(isAssembled? k-1 : 0)).collectAsMap();

        for (Map.Entry<Integer, Long> id_len : id_lengths.entrySet())
            setConfsLong("len_s" + id_len.getKey(), id_len.getValue());

        // counting 1-mer: countA_<id>, countC_<id>, countG_<id>, countT_<id>
        Map<Integer, long[]> counts_1mers = input.
                mapToPair(new Map1mers(isAssembled? k-1 : 0)).
                reduceByKey(new Reduce1mers()).
                collectAsMap();

        for (Map.Entry<Integer, long[]> id_1mers : counts_1mers.entrySet()) {
            setConfsLong("countA_s" + id_1mers.getKey(), id_1mers.getValue()[0]);
            setConfsLong("countC_s" + id_1mers.getKey(), id_1mers.getValue()[1]);
            setConfsLong("countG_s" + id_1mers.getKey(), id_1mers.getValue()[2]);
            setConfsLong("countT_s" + id_1mers.getKey(), id_1mers.getValue()[3]);
        }

        long distinct;
        Map<SequenceId, Long> seq_distincts;
        Map<SequenceId, Long> seq_pseudocounts;
        Map<SequenceId, Double> stdevs;

        if (getConf().getString("filter_regex") != null)
            filterRegex = getConf().getString("filter_regex");

        if (getConf().getString("aggr_filter_regex") != null)
            aggregatedFilterRegex = getConf().getString("aggr_filter_regex");

        try {
            if (strategy == Strategy.PARTIAL_AGGREGATION) {
                JavaPairRDD<BinId, Tuple2<SequenceId, Data>> extraction = DistributedStatisticExtractor.extractByBin(input, (StatisticExtractorByBin) extractor);
                extraction = DistributedStatisticFilter.filterByBin(extraction, filterRegex);

                if (getConf().getBoolean("bin_sizes", Defaults.BIN_SIZES)) {
                    try {
                        FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
                        FSDataOutputStream out = fs.create(new Path("bin_sizes_" + System.currentTimeMillis() + ".csv"));

                        Map<BinId, Long> bin_sizes = extraction.aggregateByKey(0L, new SumSize1(), new SumSize2()).collectAsMap();
                        out.writeBytes("binID,size\n");
                        for (Map.Entry<BinId, Long> bin_size : bin_sizes.entrySet())
                            out.writeBytes(bin_size.getKey() + "," + bin_size.getValue() + "\n");

                        out.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if (aggregator instanceof StatisticAggregatorByBinToSeqPair) {
                    JavaPairRDD<SequencePair, Value> aggregation = DistributedStatisticAggregator.aggregateByBinToSeqPair(extraction, (StatisticAggregatorByBinToSeqPair) aggregator);
                    aggregation = DistributedAggregatedStatisticFilter.filterByBinToSeqPair(aggregation, aggregatedFilterRegex);
                    distance_matrices = DistributedAFFunctionEvaluator.evaluateBySeqPair(aggregation, evaluators);
                } else {
                    JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> aggregation = DistributedStatisticAggregator.aggregateByBin(extraction, (StatisticAggregatorByBin) aggregator);
                    aggregation = DistributedAggregatedStatisticFilter.filterByBin(aggregation, aggregatedFilterRegex).persist(StorageLevel.DISK_ONLY());

                    // start: computing additional info
                    distinct = aggregation.keys().distinct().count();
                    setConfsLong("distinct", distinct);

                    seq_distincts = aggregation.mapToPair(el -> new Tuple2<>(el._2._1, el._1)).distinct().countByKey();

                    for (Map.Entry<SequenceId, Long> seq_distinct : seq_distincts.entrySet())
                        setConfsDouble("distinct_s" + seq_distinct.getKey().id, seq_distinct.getValue());

                    if (aggregation.take(1).get(0)._2._2 instanceof CountValue) {
                        seq_pseudocounts = aggregation.filter(t -> ((CountValue) t._2._2).count == 0).
                                mapToPair(t -> new Tuple2<>(t._2._1(), t._2._2)).aggregateByKey(0L, (v, c) -> v + 1, Long::sum).collectAsMap();

                        for (Map.Entry<SequenceId, Long> seq_pseudocount : seq_pseudocounts.entrySet())
                            setConfsLong("pseudo_s" + seq_pseudocount.getKey().id, seq_pseudocount.getValue());

                        stdevs = aggregation
                                .mapToPair(new StDevByStatistic(conf))
                                .reduceByKey(Double::sum)
                                .mapValues(new StDevFinalize(conf))
                                .collectAsMap();

                        for (Map.Entry<SequenceId, Double> seq_stdev : stdevs.entrySet())
                            setConfsDouble("stdev_s" + seq_stdev.getKey().id, seq_stdev.getValue());
                    }
                    // end: computing additional info

                    distance_matrices = DistributedAFFunctionEvaluator.evaluateByStatistic(aggregation, evaluators);
                    aggregation.unpersist();
                }
            } else if (strategy == Strategy.NO_AGGREGATION) {
                JavaPairRDD<Statistic, Tuple2<SequenceId, Data>> extraction = DistributedStatisticExtractor.extractByStatistic(input, (StatisticExtractorByStatistic) extractor);
                extraction = DistributedStatisticFilter.filterByStatistic(extraction, filterRegex);
                JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> aggregation = DistributedStatisticAggregator.aggregateByStatistic(extraction, (StatisticAggregatorByStatistic) aggregator);
                aggregation = DistributedAggregatedStatisticFilter.filterByStatistic(aggregation, aggregatedFilterRegex).persist(StorageLevel.DISK_ONLY());

                // start: computing additional info
                distinct = aggregation.keys().distinct().count();
                setConfsLong("distinct", distinct);

                seq_distincts = aggregation.mapToPair(el -> new Tuple2<>(el._2._1, el._1)).distinct().countByKey();

                for (Map.Entry<SequenceId, Long> seq_distinct : seq_distincts.entrySet())
                    setConfsDouble("distinct_s" + seq_distinct.getKey().id, seq_distinct.getValue());

                if (aggregation.take(1).get(0)._2._2 instanceof CountValue) {
                    seq_pseudocounts = aggregation.filter(t -> ((CountValue) t._2._2).count == 0).
                            mapToPair(t -> t._2).aggregateByKey(0L, (v, c) -> v + 1, Long::sum).collectAsMap();

                    for (Map.Entry<SequenceId, Long> seq_pseudocount : seq_pseudocounts.entrySet())
                        setConfsLong("pseudo_s" + seq_pseudocount.getKey().id, seq_pseudocount.getValue());

                    stdevs = aggregation
                            .mapToPair(new StDevByStatistic(conf))
                            .reduceByKey(Double::sum)
                            .mapValues(new StDevFinalize(conf))
                            .collectAsMap();

                    for (Map.Entry<SequenceId, Double> seq_stdev : stdevs.entrySet())
                        setConfsDouble("stdev_s" + seq_stdev.getKey().id, seq_stdev.getValue());
                }
                // end: computing additional info

                distance_matrices = DistributedAFFunctionEvaluator.evaluateByStatistic(aggregation, evaluators);
                aggregation.unpersist();
            } else if (strategy == Strategy.TOTAL_AGGREGATION) {
                JavaPairRDD<UniqueId, Tuple2<SequenceId, Data>> extraction = DistributedStatisticExtractor.extractByUnique(input, (StatisticExtractorByUnique) extractor);
                extraction = DistributedStatisticFilter.filterByUnique(extraction, filterRegex);
                JavaPairRDD<UniqueId, Tuple3<SequenceId, Statistic, Value>> aggregation = DistributedStatisticAggregator.aggregateByUnique(extraction, (StatisticAggregatorByUnique) aggregator);
                aggregation = DistributedAggregatedStatisticFilter.filterByUnique(aggregation, filterRegex).persist(StorageLevel.MEMORY_ONLY());

                // start: computing additional info
                distinct = aggregation.map(item -> item._2._2()).distinct().count();
                setConfsLong("distinct", distinct);

                seq_distincts = aggregation.mapToPair(el -> new Tuple2<>(el._2._1(), el._2._2())).distinct().countByKey();

                for (Map.Entry<SequenceId, Long> seq_distinct : seq_distincts.entrySet())
                    setConfsDouble("distinct_s" + seq_distinct.getKey().id, seq_distinct.getValue());

                if (aggregation.take(1).get(0)._2._3() instanceof CountValue) {
                    seq_pseudocounts = aggregation.filter(t -> ((CountValue) t._2._3()).count == 0).
                            mapToPair(t -> new Tuple2<>(t._2._1(), t._2._3())).aggregateByKey(0L, (v, c) -> v + 1, Long::sum).collectAsMap();

                    for (Map.Entry<SequenceId, Long> seq_pseudocount : seq_pseudocounts.entrySet())
                        setConfsLong("pseudo_s" + seq_pseudocount.getKey().id, seq_pseudocount.getValue());

                    stdevs = aggregation
                            .mapToPair(new StDevByUnique(conf))
                            .reduceByKey(Double::sum)
                            .mapValues(new StDevFinalize(conf))
                            .collectAsMap();

                    for (Map.Entry<SequenceId, Double> seq_stdev : stdevs.entrySet())
                        setConfsDouble("stdev_s" + seq_stdev.getKey().id, seq_stdev.getValue());
                }
                // end: computing additional info

                distance_matrices = DistributedAFFunctionEvaluator.evaluateByUnique(aggregation, evaluators);
                aggregation.unpersist();
            }
        }
        catch (ClassCastException e) {
            throw new AggregationStrategyException(strategy.toString());
        }

        if (outputDir != null) {
            try {
                saveDistance(ioManager.addLabels(distance_matrices), new Path(outputDir + ".csv"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveDistance(JavaPairRDD<Tuple2<String, String>, AFValue[]> distance_matrix_rdd, Path path) throws IOException {
        FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
        FSDataOutputStream out = fs.create(path.suffix("_tmp"));

        out.writeBytes("SEQ1,SEQ2");

        for (String name : getEvaluatorNames())
            out.writeBytes("," + name);

        out.writeByte('\n');

        for (Map.Entry<Tuple2<String, String>, AFValue[]> seqs_dist : distance_matrix_rdd.collectAsMap().entrySet())
            out.writeBytes(seqs_dist.getKey()._1 + "," + seqs_dist.getKey()._2 + "," + array2String(seqs_dist.getValue()) + "\n");

        out.close();
        fs.rename(path.suffix("_tmp"), path);
    }

    public void setInput(JavaRDD<Chunk> input_rdd) {
        input = input_rdd;
    }

    public void setInput(String inputPath) {
        this.inputPath = inputPath;

        ioManager = new IOManager();

        String assembled = conf.getString("assembled", Defaults.ASSEMBLED);
        String type = conf.getString("type", Defaults.TYPE);

        if(assembled.equals("no")) { // unassembled
            if (type.equals("fasta")) // fasta
                ioManager.readUnassembledFasta(inputPath, conf.getInt("buffer_size", Defaults.BUFFER_SIZE), conf.getInt("k"), jsc);
            else // fastq
                ioManager.readUnassembledFastq(inputPath, conf.getInt("buffer_size", Defaults.BUFFER_SIZE), conf.getInt("k"), jsc);
        }
        else { // assembled (FASTA)
            if (assembled.equals("long")) // long
                ioManager.readAssembledLong(inputPath, conf.getInt("k"), jsc);
            else // short (FASTA)
                ioManager.readAssembledShort(inputPath, conf.getInt("buffer_size", Defaults.BUFFER_SIZE), jsc);
        }

        int split_size = conf.getInt("split_size", Defaults.SPLIT_SIZE);

        if (split_size > 0)
            ioManager.split(split_size, conf.getInt("k"));

        ioManager.chunks_rdd = ioManager.chunks_rdd.repartition(conf.getInt("slices", Defaults.SLICES));

        setConfsInt("n", ioManager.id2key_bc.getValue().size());
        input = ioManager.chunks_rdd;
    }

    void setStatisticExtractor(Class cls, Configuration conf) {
        if (!StatisticExtractor.class.isAssignableFrom(cls))
            throw new RuntimeException("The class does not extends StatisticExtractor");

        try {
            extractor = (StatisticExtractor) cls.getDeclaredConstructor(Configuration.class).newInstance(conf);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public void setStatisticExtractor(Class cls) {
        setStatisticExtractor(cls, conf);
    }

    void setStatisticAggregator(Class cls, Configuration conf) {
        if (!StatisticAggregator.class.isAssignableFrom(cls))
            throw new RuntimeException("The class does not extends StatisticAggregator");

        try {
            aggregator = (StatisticAggregator) cls.getDeclaredConstructor(Configuration.class).newInstance(conf);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public void setStatisticAggregator(Class cls) {
        setStatisticAggregator(cls, conf);
    }

    void addAFFunctionEvaluator(Class cls, Configuration conf) {
        if (!AFFunctionEvaluator.class.isAssignableFrom(cls))
            throw new RuntimeException("The class does not extends AFFunctionEvaluator");

        try {
            evaluators.add(((Class<? extends AFFunctionEvaluator>) cls).getDeclaredConstructor(Configuration.class).newInstance(conf));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public void addAFFunctionEvaluator(Class cls) {
        addAFFunctionEvaluator(cls, conf);
    }

    public void setOutput(String outputDir) {
        this.outputDir = outputDir;
    }

    public String getInputPath() {
        return inputPath;
    }

    public JavaPairRDD<SequencePair, AFValue[]> getDistances() {
        return distance_matrices;
    }

    public void close() {
        jsc.close();
    }

    public Configuration getConf() {
        return conf;
    }

    public JavaRDD<Chunk> getInput() {
        return input;
    }

    public void setFilterRegex(String regex) {
        filterRegex = regex;
    }

    public void setAggregatedFilterRegex(String regex) {
        aggregatedFilterRegex = regex;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public Broadcast<List<String>> getLabels() {
        return ioManager.id2key_bc;
    }

    public String[] getEvaluatorNames() {
        return evaluators.stream().map(evaluator -> evaluator.getClass().getSimpleName().toLowerCase()).toArray(String[]::new);
    }

    public ArrayList<AFFunctionEvaluator> getDistanceEvaluators() {
        return evaluators;
    }

    public JavaSparkContext getJavaSparkContext() {
        return jsc;
    }

    public String getOutputPath() {
        return outputDir;
    }

    private String array2String(Object[] array) {
        assert array.length > 0;

        StringBuilder s = new StringBuilder(array[0].toString());

        for (int i = 1; i < array.length; i++)
            s.append(",").append(array[i].toString());

        return s.toString();
    }

    public void setConfsString(String key, String value) {
        conf.setString(key, value);

        if (extractor != null) extractor.getConf().setString(key, value);
        if (aggregator != null) aggregator.getConf().setString(key, value);

        for (AFFunctionEvaluator evaluator : evaluators)
            evaluator.getConf().setString(key, value);
    }

    public void setConfsInt(String key, int value) {
        conf.setInt(key, value);

        if (extractor != null) extractor.getConf().setInt(key, value);
        if (aggregator != null) aggregator.getConf().setInt(key, value);

        for (AFFunctionEvaluator evaluator : evaluators)
            evaluator.getConf().setInt(key, value);
    }

    public void setConfsBoolean(String key, boolean value) {
        conf.setBoolean(key, value);

        if (extractor != null) extractor.getConf().setBoolean(key, value);
        if (aggregator != null) aggregator.getConf().setBoolean(key, value);

        for (AFFunctionEvaluator evaluator : evaluators)
            evaluator.getConf().setBoolean(key, value);
    }

    public void setConfsDouble(String key, double value) {
        conf.setDouble(key, value);

        if (extractor != null) extractor.getConf().setDouble(key, value);
        if (aggregator != null) aggregator.getConf().setDouble(key, value);

        for (AFFunctionEvaluator evaluator : evaluators)
            evaluator.getConf().setDouble(key, value);
    }

    public void setConfsLong(String key, long value) {
        conf.setLong(key, value);

        if (extractor != null) extractor.getConf().setLong(key, value);
        if (aggregator != null) aggregator.getConf().setLong(key, value);

        for (AFFunctionEvaluator evaluator : evaluators)
            evaluator.getConf().setLong(key, value);
    }
}
