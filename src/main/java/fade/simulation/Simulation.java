package fade.simulation;

import fade.exception.AggregationStrategyException;
import fade.AFFunctionEvaluator;
import fade.util.*;
import fade.util.AFValue;
import fade.Fade;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Simulation {
	private final Fade fade;
	private final Configuration conf;
	private final JavaSparkContext jsc;

	public Simulation(Fade fade) {
		this(fade, fade.getJavaSparkContext(), fade.getConf());
	}

	public Simulation(Fade fade, JavaSparkContext jsc, Configuration conf) {
		this.fade = fade;
		this.jsc = jsc;
		this.conf = conf;
	}

	@SuppressWarnings("rawtypes")
	public void simulate() throws IllegalArgumentException, IOException, AggregationStrategyException {
		String input_name = new Path(fade.getInputPath()).getName();
		String output = fade.getOutputPath();
		fade.setOutput(null); // no saving enabled
		FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());

		int n_simulations = conf.getInt("simulations", Defaults.SIMULATIONS);
		int q = conf.getInt("q", Defaults.Q);
		int k = conf.getInt("k");
		double alpha = conf.getDouble("alpha", Defaults.ALPHA);
		boolean significance = conf.getBoolean("significance", Defaults.SIGNIFICANCE);

		int slices = conf.getInt("slices", Defaults.SLICES);
		int n = conf.getInt("n");
		long n_hypothesis = n * (n - 1) / 2;
		int rank_thresh = (int) Math.ceil(alpha / n_hypothesis * n_simulations);
		String[] evaluator_names = fade.getEvaluatorNames();
		int n_evaluators = evaluator_names.length;
		boolean[] isSimilarity = new boolean[n_evaluators];

		int s = 0;
		for (AFFunctionEvaluator evaluator : fade.getDistanceEvaluators())
			isSimilarity[s++] = evaluator instanceof Similarity;

		HashMap<SequencePair, Rank[]> rankings_map = new HashMap<>();

		for (int i = 0; i < n - 1; i++)
			for (int j = i + 1; j < n; j++) {
				Rank[] ranks = new Rank[n_evaluators];

				for (int e = 0; e < evaluator_names.length; e++)
					ranks[e] = new Rank(-1);

				rankings_map.put(new SequencePair(i, j), ranks);
			}

		Map<SequencePair, AFValue[]> orig_dists_map = loadDistance(getDistancePath(input_name), evaluator_names, fs);

		if (orig_dists_map.isEmpty()) {
			fade.compute();
			orig_dists_map = fade.getDistances().collectAsMap();
			saveDistance(orig_dists_map, getDistancePath(input_name), evaluator_names, fs);
		}
		
		for (SequencePair pair : orig_dists_map.keySet()) {
			Rank[] ranks = rankings_map.get(pair);
			for (Rank rank : ranks) {
				rank.lower();
				rank.lower(); // rank = 1
			}
		}

		JavaRDD<Chunk> original = fade.getInput().persist(StorageLevel.DISK_ONLY());

		int sim = 0;
		while (sim++ < n_simulations) {
			Map<SequencePair, AFValue[]> rnd_dists_map = loadDistance(getDistancePath(input_name, q, sim), evaluator_names, fs);

			if (rnd_dists_map.isEmpty()) {
				Path path = getRandomPath(input_name, q, sim);
				JavaRDD<Chunk> rnd;
				if (fs.exists(path) && fs.getContentSummary(path).getFileCount() > 0) {
					rnd = jsc.objectFile(path.toString(), slices);
				}
				else {
					if (fs.exists(path))
						fs.delete(path, true);

					rnd = Randomizer.randomize(original, q, n, k, jsc);
					rnd.saveAsObjectFile(path.toString());
				}

				fade.setInput(rnd);
				fade.compute();
				rnd_dists_map = fade.getDistances().collectAsMap();
				saveDistance(rnd_dists_map, getDistancePath(input_name, q, sim), evaluator_names, fs);
			}

			for (SequencePair pair : rnd_dists_map.keySet()) {
				AFValue[] orig_dists = orig_dists_map.get(pair);
				AFValue[] rnd_dists = rnd_dists_map.get(pair);

				if (orig_dists != null && rnd_dists != null)
					for (int i = 0; i < n_evaluators; i++) {
						if (isSimilarity[i]) {
							if (rnd_dists[i].value > orig_dists[i].value)
								rankings_map.get(pair)[i].lower();
						}
						else // is not a similarity function
							if (rnd_dists[i].value < orig_dists[i].value)
							rankings_map.get(pair)[i].lower();
					}
			}
		}

		original.unpersist();

		if (significance)
			for (Rank[] ranks : rankings_map.values())
				for (Rank rank : ranks)
					rank.value = rank.value <= rank_thresh ? 1 : 0;


		saveRanking(rankings_map, new Path(output + ".csv"), evaluator_names, fs);
	}

	private void saveDistance(Map<SequencePair, AFValue[]> distance_map, Path path, String[] fade_evaluator_names, FileSystem fs) throws IOException {
		Path tmp_path = path.suffix("_tmp");

		if (fs.exists(tmp_path))
			fs.delete(tmp_path, false);

		FSDataOutputStream out = fs.create(tmp_path);

		out.writeBytes("SEQ1,SEQ2");

		for (String name : fade_evaluator_names)
			out.writeBytes("," + name);

		out.writeByte('\n');

		for (Map.Entry<SequencePair, AFValue[]> seqs_dist : distance_map.entrySet())
			out.writeBytes(seqs_dist.getKey().id1 + "," + seqs_dist.getKey().id2 + "," + array2String(seqs_dist.getValue()) + "\n");

		out.close();
		fs.rename(tmp_path, path);
	}

	private Map<SequencePair, AFValue[]> loadDistance(Path path, String[] fade_evaluator_names, FileSystem fs) {
		Map<SequencePair, AFValue[]> distance_map = new HashMap<>();

		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));

			// header
			String line = in.readLine();

			String[] id1_id2_dists = line.trim().split(",");
			String[] evaluator_names = new String[id1_id2_dists.length-2];

			System.arraycopy(id1_id2_dists, 2, evaluator_names, 0, evaluator_names.length);

			boolean found;
			for (String fname : fade_evaluator_names) {
				found = false;
				for (String name : evaluator_names)
					if (fname.equals(name)) {
						found = true;
						break;
					}
				if (!found)
					return distance_map;
			}

			while ((line = in.readLine()) != null) {
				id1_id2_dists = line.trim().split(",");
				int id1 = Integer.parseInt(id1_id2_dists[0]);
				int id2 = Integer.parseInt(id1_id2_dists[1]);
				AFValue[] AFValues = new AFValue[id1_id2_dists.length-2];

				for (int i = 0; i < evaluator_names.length; i++)
					AFValues[i] = new AFValue(Double.parseDouble(id1_id2_dists[i+2]));

				distance_map.put(new SequencePair(id1, id2), AFValues);
			}

			in.close();
		} catch (IOException e) {
			return distance_map;
		}
		
		return distance_map;
	}

	private void saveRanking(Map<SequencePair, Rank[]> ranking_map, Path path, String[] fade_evaluator_names, FileSystem fs) throws IOException {
		FSDataOutputStream out = fs.create(path.suffix("_tmp"));

		out.writeBytes("SEQ1,SEQ2");

		for (String name : fade_evaluator_names)
			out.writeBytes(',' + name);

		out.writeByte('\n');

		for (Map.Entry<SequencePair, Rank[]> seqs_ranks : ranking_map.entrySet())
			out.writeBytes(seqs_ranks.getKey().id1 + "," + seqs_ranks.getKey().id2 + "," + array2String(seqs_ranks.getValue()) + "\n");

		out.close();
		fs.rename(path.suffix("_tmp"), path);
	}

	private Path getDistancePath(Object... ids) {
		StringBuilder path = new StringBuilder(Defaults.SIM_DISTANCES_PATH);

		for (Object id : ids)
			path.append('_').append(id.toString());

		return new Path(path + ".csv");
	}

	private Path getRandomPath(Object... ids) {
		StringBuilder path = new StringBuilder(Defaults.SIM_RANDOMS_PATH);

		for (Object id : ids)
			path.append('_').append(id.toString());

		return new Path(path.toString());
	}

	private String array2String(Object[] array) {
		assert array.length > 0;

		StringBuilder s = new StringBuilder(array[0].toString());

		for (int i = 1; i < array.length; i++)
			s.append(",").append(array[i].toString());

		return s.toString();
	}
}
