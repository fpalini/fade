package fade;

import fade.util.AFValue;
import fade.util.Value;
import fade.util.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

abstract class DistributedAFFunctionEvaluator {

	static JavaPairRDD<SequencePair, AFValue[]> evaluateByStatistic(JavaPairRDD<Statistic, Tuple2<SequenceId, Value>> input, ArrayList<AFFunctionEvaluator> evaluators) {
		return input
				.groupByKey()
				.flatMapToPair(item -> {
					Iterator<Tuple2<SequenceId, Value>> iter1 = item._2.iterator();
					Iterator<Tuple2<SequenceId, Value>> iter2;

					ArrayList<Tuple2<SequencePair, AFValue[]>> partialDistances = new ArrayList<>();
					AFValue[] AFValues;
					int k = evaluators.get(0).getConf().getInt("k");

					for (AFFunctionEvaluator e : evaluators) {
						e.setDistinct(e.getConf().getLong("distinct"));
						e.setStatistic(item._1);
					}

					while (iter1.hasNext()) {
						Tuple2<SequenceId, Value> v1 = iter1.next();
						iter2 = item._2.iterator();

						for (int i = 0; i < evaluators.size(); i++) {
							AFFunctionEvaluator e = evaluators.get(i);

							e.setCount1(e.getConf().getLong("len_s" + v1._1.id) - k+1);
							e.setCountA1(e.getConf().getLong("countA_s" + v1._1.id));
							e.setCountC1(e.getConf().getLong("countC_s" + v1._1.id));
							e.setCountG1(e.getConf().getLong("countG_s" + v1._1.id));
							e.setCountT1(e.getConf().getLong("countT_s" + v1._1.id));
							e.setDistinct1(e.getConf().getLong("distinct_s" + v1._1.id));

							if (e.getConf().getDouble("stdev_s" + v1._1.id) != null) {
								e.setPseudo1(e.getConf().getLong("pseudo_s" + v1._1.id, 0));
								e.setStdev1(e.getConf().getDouble("stdev_s" + v1._1.id));
							}
						}

						while (iter2.hasNext()) {
							Tuple2<SequenceId, Value> v2 = iter2.next();

 							// calcola la distanza solo per le sequenze adiacenti
 							if (v1._1.id % 2 == 0 && v1._1.id + 1 == v2._1.id) {
 							// if (v1._1.id < v2._1.id) {
  
							// System.out.printf("filtered: %d - %d\n", v1._1.id, v2._1.id);

							    AFValues = new AFValue[evaluators.size()];

								for (int i = 0; i < evaluators.size(); i++) {
									AFFunctionEvaluator e = evaluators.get(i);

									e.setCount2(e.getConf().getLong("len_s" + v2._1.id) - k+1);
									e.setCountA2(e.getConf().getLong("countA_s" + v2._1.id));
									e.setCountC2(e.getConf().getLong("countC_s" + v2._1.id));
									e.setCountG2(e.getConf().getLong("countG_s" + v2._1.id));
									e.setCountT2(e.getConf().getLong("countT_s" + v2._1.id));
									e.setDistinct2(e.getConf().getLong("distinct_s" + v2._1.id));

									if (e.getConf().getDouble("stdev_s" + v2._1.id) != null) {
										e.setPseudo2(e.getConf().getLong("pseudo_s" + v2._1.id, 0));
										e.setStdev2(e.getConf().getDouble("stdev_s" + v2._1.id));
									}

									AFValues[i] = e.evaluatePartialAFValue(v1._2, v2._2);
								}

								partialDistances.add(new Tuple2<>(new SequencePair(v1._1, v2._1), AFValues));
 								break; // valutata la coppia si passa alla prossima seq1
							}
						}
					}

					return partialDistances.iterator();
				})
				.reduceByKey((dist1, dist2) -> {
					AFValue[] AFValues = new AFValue[evaluators.size()];

					for (int i = 0; i < evaluators.size(); i++)
						AFValues[i] = evaluators.get(i).combinePartialAFValues(dist1[i], dist2[i]);

					return AFValues;
				})
				.mapToPair(seq_distances -> {
					int k = evaluators.get(0).getConf().getInt("k");

					for (AFFunctionEvaluator e : evaluators)
						e.setDistinct(e.getConf().getLong("distinct"));

					for (int i = 0; i < evaluators.size(); i++) {
						AFFunctionEvaluator e = evaluators.get(i);

						e.setCount1(e.getConf().getLong("len_s" + seq_distances._1.id1) - k+1);
						e.setDistinct1(e.getConf().getLong("distinct_s" + seq_distances._1.id1));
						e.setCountA1(e.getConf().getLong("countA_s" + seq_distances._1.id1));
						e.setCountC1(e.getConf().getLong("countC_s" + seq_distances._1.id1));
						e.setCountG1(e.getConf().getLong("countG_s" + seq_distances._1.id1));
						e.setCountT1(e.getConf().getLong("countT_s" + seq_distances._1.id1));

						e.setCount2(e.getConf().getLong("len_s" + seq_distances._1.id2) - k+1);
						e.setDistinct2(e.getConf().getLong("distinct_s" + seq_distances._1.id2));
						e.setCountA2(e.getConf().getLong("countA_s" + seq_distances._1.id2));
						e.setCountC2(e.getConf().getLong("countC_s" + seq_distances._1.id2));
						e.setCountG2(e.getConf().getLong("countG_s" + seq_distances._1.id2));
						e.setCountT2(e.getConf().getLong("countT_s" + seq_distances._1.id2));

						if (e.getConf().getDouble("stdev_s" + seq_distances._1.id1) != null) {
							e.setPseudo1(e.getConf().getLong("pseudo_s" + seq_distances._1.id1, 0));
							e.setStdev1(e.getConf().getDouble("stdev_s" + seq_distances._1.id1));
							
							e.setStdev2(e.getConf().getDouble("stdev_s" + seq_distances._1.id2));
							e.setPseudo2(e.getConf().getLong("pseudo_s" + seq_distances._1.id2, 0));
						}

						seq_distances._2[i] = e.finalizeAFValue(seq_distances._2[i]);
					}

					return seq_distances;
				});
	}

	static JavaPairRDD<SequencePair, AFValue[]> evaluateByUnique(JavaPairRDD<UniqueId, Tuple3<SequenceId, Statistic, Value>> input, ArrayList<AFFunctionEvaluator> evaluators) {
		return input
				.mapPartitionsToPair(item -> {
					List<Tuple2<UniqueId, Tuple3<SequenceId, Statistic, Value>>> items = IteratorUtils.toList(item);
					items.sort((t1, t2) -> Integer.compare(t1._2._2().getHashCode(), t2._2._2().getHashCode()));

					HashMap<SequencePair, AFValue[]> partialDistancesMap = new HashMap<>();
					AFValue[] AFValues;
					SequencePair pair;
					int k = evaluators.get(0).getConf().getInt("k");
					boolean isCountValue = evaluators.get(0).getConf().getDouble("stdev_s" + items.get(0)._2._1().id) != null;

					for (AFFunctionEvaluator e : evaluators)
						e.setDistinct(e.getConf().getLong("distinct"));

					for (int i1 = 0; i1 < items.size(); i1++) {
						Tuple3<SequenceId, Statistic, Value> v1 = items.get(i1)._2;

						for (int i = 0; i < evaluators.size(); i++) {
							AFFunctionEvaluator e = evaluators.get(i);

							e.setCount1(e.getConf().getLong("len_s" + v1._1().id) - k+1);
							e.setCountA1(e.getConf().getLong("countA_s" + v1._1().id));
							e.setCountC1(e.getConf().getLong("countC_s" + v1._1().id));
							e.setCountG1(e.getConf().getLong("countG_s" + v1._1().id));
							e.setCountT1(e.getConf().getLong("countT_s" + v1._1().id));
							e.setDistinct1(e.getConf().getLong("distinct_s" + v1._1().id));

							if (isCountValue) {
								e.setPseudo1(e.getConf().getLong("pseudo_s" + v1._1().id, 0));
								e.setStdev1(e.getConf().getDouble("stdev_s" + v1._1().id));
							}

							e.setStatistic(v1._2());
						}

						for (int i2 = i1+1; i2 < items.size(); i2++) {
							Tuple3<SequenceId, Statistic, Value> v2 = items.get(i2)._2;

							if (!v1._2().equals(v2._2()))
								break;

							if (v2._1().id < v1._1().id) {
								Tuple3<SequenceId, Statistic, Value> tmp = v1;
								v1 = v2;
								v2 = tmp;
							}

							pair = new SequencePair(v1._1(), v2._1());
							AFValue[] prev_AFValues = partialDistancesMap.get(pair);
							AFValues = new AFValue[evaluators.size()];

							for (int i = 0; i < evaluators.size(); i++) {
								AFFunctionEvaluator e = evaluators.get(i);

								e.setCount2(e.getConf().getLong("len_s" + v2._1().id) - k+1);
								e.setCountA2(e.getConf().getLong("countA_s" + v2._1().id));
								e.setCountC2(e.getConf().getLong("countC_s" + v2._1().id));
								e.setCountG2(e.getConf().getLong("countG_s" + v2._1().id));
								e.setCountT2(e.getConf().getLong("countT_s" + v2._1().id));
								e.setDistinct2(e.getConf().getLong("distinct_s" + v2._1().id));

								if (isCountValue) {
									e.setPseudo2(e.getConf().getLong("pseudo_s" + v2._1().id, 0));
									e.setStdev2(e.getConf().getDouble("stdev_s" + v2._1().id));
								}

								AFValues[i] = e.evaluatePartialAFValue(v1._3(), v2._3());

								if (prev_AFValues == null)
									partialDistancesMap.put(pair, AFValues);
								else
									prev_AFValues[i] = e.combinePartialAFValues(prev_AFValues[i], AFValues[i]);
							}
						}
					}

					ArrayList<Tuple2<SequencePair, AFValue[]>> partialDistances = new ArrayList<>();

					for (Map.Entry<SequencePair, AFValue[]> kv : partialDistancesMap.entrySet()) {
						pair = kv.getKey();
						AFValues = kv.getValue();

						for (int i = 0; i < evaluators.size(); i++)
							AFValues[i] = evaluators.get(i).finalizeAFValue(AFValues[i]);

						partialDistances.add(new Tuple2<>(pair, AFValues));
					}

					return partialDistances.iterator();
				});
	}

    static JavaPairRDD<SequencePair, AFValue[]> evaluateBySeqPair(JavaPairRDD<SequencePair, Value> input, ArrayList<AFFunctionEvaluator> evaluators) {
		return input
				.filter(x -> ((x._1.id1 % 2 == 0) && (x._1.id1 + 1 == x._1.id2)))
				.groupByKey()
				.mapToPair(item -> {
					AFValue[] AFValues = new AFValue[evaluators.size()];

					for (int i = 0; i < AFValues.length; i++)
						AFValues[i] = ((AFFunctionEvaluatorBySeqPair) evaluators.get(i)).call(item).get(0)._2;

					return new Tuple2<>(item._1, AFValues);
				})
				.mapValues(distances -> {
					for (int i = 0; i < evaluators.size(); i++)
						distances[i] = evaluators.get(i).finalizeAFValue(distances[i]);

					return distances;
				});
	}
}
