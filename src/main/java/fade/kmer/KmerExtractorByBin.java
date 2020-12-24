package fade.kmer;

import fade.StatisticExtractorByBin;
import fade.util.*;
import scala.Tuple2;

import java.util.*;

public class KmerExtractorByBin extends StatisticExtractorByBin {

    public KmerExtractorByBin(Configuration conf) {
        super(conf);
    }

    @Override
    public Collection<Tuple2<BinId, Tuple2<SequenceId, Data>>> call(Chunk chunk) {
        int k = getConf().getInt("k");
        boolean canonical = getConf().getBoolean("canonical", Defaults.CANONICAL);

        SequenceId seqId = new SequenceId(chunk.id);
        int len = chunk.data.length;
        int nBin = getConf().getInt("slices", Defaults.SLICES);

        Hashtable<Kmer, Long>[] frequencies = new Hashtable[nBin];
        KmerTool tool = new KmerTool(chunk.data, k);
        Kmer kmer;

        for (int i = 0; i < len - (k-1); i++) {
            kmer = new Kmer(canonical? tool.nextKmerCan() : tool.nextKmer(), k);
            int idBin = Math.abs(kmer.hashCode() % nBin);

            if (frequencies[idBin] == null)
                frequencies[idBin] = new Hashtable<>();

            frequencies[idBin].merge(kmer, 1L, Long::sum);
        }

        List<Tuple2<BinId, Tuple2<SequenceId, Data>>> kmers = new ArrayList<>();

        for (int idBin = 0; idBin < nBin; idBin++)
            if (frequencies[idBin] != null)
                kmers.add(new Tuple2<>(new BinId(idBin), new Tuple2<>(seqId, new KmerDataByBin(frequencies[idBin]))));

        return kmers;
    }
}
