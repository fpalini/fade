package fade.mash;

import java.util.*;

import com.google.common.primitives.Longs;
import fade.StatisticExtractorByBin;
import fade.util.*;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import org.apache.orc.util.Murmur3;
import scala.Tuple2;

public class MashExtractor extends StatisticExtractorByBin {
	
	public MashExtractor(Configuration conf)  {
    super(conf);
    }
	
	public Collection<Tuple2<BinId, Tuple2<SequenceId, Data>>> call(Chunk chunk) {
			int k = getConf().getInt("k");
			int s = getConf().getInt("s");
			boolean canonical = getConf().getBoolean("canonical", Defaults.CANONICAL);

		    int seqId = chunk.id;

	        List<Tuple2<BinId, Tuple2<SequenceId, Data>>> output = new ArrayList<>(s);

	        long[] sketches;

		    if (k < 17)
				sketches = createSketches32(chunk, k, s, canonical);
		    else
				sketches = createSketches64(chunk, k, s, canonical);

		    output.add(new Tuple2<>(new BinId(seqId),new Tuple2<>(new SequenceId(seqId), new MashData(sketches))));
		    
		    return output;
		}


	private static long[] createSketches32(Chunk chunk, int k, int s, boolean canonical) {
		int len = chunk.data.length;

		LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

		long hash;
		boolean takeCompRev;

		byte[] seqCompRev = null;

		if (canonical) {
			seqCompRev = new byte[len];

			for (int i = 0; i < len; i++)
				seqCompRev[i] = KmerTool.bitmask_comp_char[chunk.data[len - 1 - i]];
		}

		int limit = Math.min(s, len - (k-1));

		for (int i = 0; i < limit; i++) {
			// used for reads
			if (chunk.data[i+k-1] == 0) {
				i += k;

				if (i >= limit)
					break;
			}

			takeCompRev = false;

			if (canonical) {
				for (int j = 0; j < k; j++) {
					byte kmer = chunk.data[i + j];
					byte kmerCR = seqCompRev[len - k - i + j];

					if (kmer < kmerCR)
						break;
					else if (kmer > kmerCR) {
						takeCompRev = true;
						break;
					}
				}
			}

			if (takeCompRev)
				hash = (int) Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
			else
				hash = (int) Murmur3.hash128(chunk.data, i, k, 42)[0];

			hashes.add(hash);
		}

		limit = len - (k-1);

		for (int i = s; i < limit; i++) {
			// used for reads
			if (chunk.data[i+k-1] == 0) {
				i += k;

				if (i >= limit)
					break;
			}

			takeCompRev = false;

			if (canonical) {
				for (int j = 0; j < k; j++) {
					byte kmer = chunk.data[i + j];
					byte kmerCR = seqCompRev[len - k - i + j];

					if (kmer < kmerCR)
						break;
					else if (kmer > kmerCR) {
						takeCompRev = true;
						break;
					}
				}
			}

			if (takeCompRev)
				hash = (int) Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
			else
				hash = (int) Murmur3.hash128(chunk.data, i, k, 42)[0];

			if (hashes.size() < s)
				hashes.add(hash);
			else if (Long.compareUnsigned(hash, hashes.lastLong()) < 0 && !hashes.contains(hash)) {
				hashes.remove(hashes.lastLong());
				hashes.add(hash);
			}
		}

		return hashes.toLongArray();
	}

	private static long[] createSketches64(Chunk chunk, int k, int s, boolean canonical) {
		int len = chunk.data.length;

		LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

		long hash;
		boolean takeCompRev;

		byte[] seqCompRev = null;

		if (canonical) {
			seqCompRev = new byte[len];

			for (int i = 0; i < len; i++)
				seqCompRev[i] = KmerTool.bitmask_comp_char[chunk.data[len - 1 - i]];
		}

		int limit = Math.min(s, len - (k-1));

		for (int i = 0; i < limit; i++) {
			// used for reads
			if (chunk.data[i+k-1] == 0) {
				i += k;

				if (i >= limit)
					break;
			}

			takeCompRev = false;

			if (canonical) {
				for (int j = 0; j < k; j++) {
					byte kmer = chunk.data[i + j];
					byte kmerCR = seqCompRev[len - k - i + j];

					if (kmer < kmerCR)
						break;
					else if (kmer > kmerCR) {
						takeCompRev = true;
						break;
					}
				}
			}

			if (takeCompRev)
				hash = Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
			else
				hash = Murmur3.hash128(chunk.data, i, k, 42)[0];

			hashes.add(hash);
		}

		limit = len - (k-1);

		for (int i = s; i < limit; i++) {
			// used for reads
			if (chunk.data[i+k-1] == 0) {
				i += k;

				if (i >= limit)
					break;
			}

			takeCompRev = false;

			if (canonical) {
				for (int j = 0; j < k; j++) {
					byte kmer = chunk.data[i + j];
					byte kmerCR = seqCompRev[len - k - i + j];

					if (kmer < kmerCR)
						break;
					else if (kmer > kmerCR) {
						takeCompRev = true;
						break;
					}
				}
			}

			if (takeCompRev)
				hash = Murmur3.hash128(seqCompRev, len-k-i, k, 42)[0];
			else
				hash = Murmur3.hash128(chunk.data, i, k, 42)[0];

			if (hashes.size() < s)
				hashes.add(hash);
			else if (Long.compareUnsigned(hash, hashes.lastLong()) < 0 && !hashes.contains(hash)) {
				hashes.remove(hashes.lastLong());
				hashes.add(hash);
			}
		}

		return hashes.toLongArray();
	}

	private static long[] createSketches32long(Chunk chunk, int k, int s) {
		int len = chunk.data.length;

		LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

		long hash;

		KmerTool tool = new KmerTool(chunk.data, k);

		for (int i = 0; i < Math.min(s, len - (k-1)); i++) {
			long kmer = tool.nextKmerCan();

			hash = (int) Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

			hashes.add(hash);
		}

		for (int i = s; i < len - (k-1); i++) {
			long kmer = tool.nextKmerCan();

			hash = (int) Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

			if (Long.compareUnsigned(hash, hashes.lastLong()) < 0) {
				hashes.remove(hashes.lastLong());
				hashes.add(hash);
			}
		}

		return hashes.toLongArray();
	}

	private static long[] createSketches64long(Chunk chunk, int k, int s) {
		int len = chunk.data.length;

		LongAVLTreeSet hashes = new LongAVLTreeSet(Long::compareUnsigned);

		long hash;

		KmerTool tool = new KmerTool(chunk.data, k);

		for (int i = 0; i < Math.min(s, len - (k-1)); i++) {
			long kmer = tool.nextKmerCan();

			hash = Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

			hashes.add(hash);
		}

		for (int i = s; i < len - (k-1); i++) {
			long kmer = tool.nextKmerCan();

			hash = Murmur3.hash128(Longs.toByteArray(kmer), 0, 8, 42)[0];

			if (Long.compareUnsigned(hash, hashes.lastLong()) < 0) {
				hashes.remove(hashes.lastLong());
				hashes.add(hash);
			}
		}

		return hashes.toLongArray();
	}

	public static long[] merge(long[] a, long[] b, int s) {
		long[] c = new long[s];
		int i = 0, j = 0, k = 0;

		while (k < s && i < a.length && j < b.length) {
			c[k++] = Long.compareUnsigned(a[i], b[j]) < 1 ? a[i++] : b[j++];

			if (i < a.length && c[k-1] == a[i])
				i++;

			if (j < b.length && c[k-1] == b[j])
				j++;
		}

		while (k < s && i < a.length) {
			c[k++] = a[i++];

			if (i < a.length && c[k-1] == a[i])
				i++;
		}

		while (k < s && j < b.length) {
			c[k++] = b[j++];

			if (j < b.length && c[k-1] == b[j])
				j++;
		}

		return Arrays.copyOf(c, k);
	}
}
		
		



