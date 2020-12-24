package fade.sw;

import fade.*;
import fade.util.Value;
import fade.util.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SwAggregatorByBin extends StatisticAggregatorByBinToSeqPair {

    private final int n, n_dontcares, threshold;
    private final int[][] score0, scoreMod;
    private final byte[][] mis0, misMod;

    public SwAggregatorByBin(Configuration conf) {
        super(conf);
        String pattern = conf.get("pattern");
        n = conf.getInt("n");
        threshold = conf.getInt("threshold");

        int l = pattern.length();

        int w = 0;

        for (int i = 0; i < l; i++)
            if (pattern.charAt(i) == '1')
                w++;

        n_dontcares = l-w;
        int n_dontcares_mod = n_dontcares % 4;

        score0 = SwUtils.Chiaromonte[0];
        scoreMod = SwUtils.Chiaromonte[n_dontcares_mod];
        mis0 = SwUtils.mismatches[0];
        misMod = SwUtils.mismatches[n_dontcares_mod];
    }

    @Override
    public List<Tuple2<SequencePair, Value>> call(Tuple2<BinId, Iterable<Tuple2<SequenceId, Data>>> W_list) {
        List<Tuple2<SequencePair, Value>> mismatches_list = new ArrayList<>();

        Iterator<Tuple2<SequenceId, Data>> iter1, iter2;
        Tuple2<SequenceId, Data> id_sw1, id_sw2;
        SwData sw1, sw2;

        int i, score, nMismatches;

        ArrayList<Match> matches = new ArrayList<>();

        Match m1, m2;
        long mismatches, lenghts;
        int id2_mod;

        for (int id1 = 0; id1 < n; id1++)
            for (int id2 = id1+1; id2 < n; id2++) {
                iter1 = W_list._2.iterator();

                while (iter1.hasNext()) {
                    id_sw1 = iter1.next();

                    if (id_sw1._1.id != id1)
                        continue;

                    sw1 = (SwData) id_sw1._2;
                    iter2 = W_list._2.iterator();

                    matches.clear();

                    while (iter2.hasNext()) {
                        id_sw2 = iter2.next();
                        id2_mod = id_sw2._1.id;

                        // reverse
                        if (id2_mod >= n)
                            id2_mod -= n;

                        if (id2_mod != id2)
                            continue;

                        sw2 = (SwData) id_sw2._2;

                        score = nMismatches = 0;

                        for (i = 0; i < sw1.dontcares.length - 1; i++) {
                            score += score0[sw1.dontcares[i] & 0xFF][sw2.dontcares[i] & 0xFF];
                            nMismatches += mis0[sw1.dontcares[i] & 0xFF][sw2.dontcares[i] & 0xFF];
                        }

                        score += scoreMod[sw1.dontcares[i] & 0xFF][sw2.dontcares[i] & 0xFF];
                        nMismatches += misMod[sw1.dontcares[i] & 0xFF][sw2.dontcares[i] & 0xFF];

                        if (score > threshold)
                            matches.add(new Match(sw1.offset, sw2.offset, nMismatches, score));
                    }

                    if (matches.isEmpty()) continue;

                    matches.sort((match1, match2) -> -Integer.compare(match1.score, match2.score));

                    mismatches = lenghts = 0;

                    for (int i1 = 0; i1 < matches.size(); i1++) {
                        m1 = matches.get(i1);

                        if (m1.pos1 != -1 && m1.pos2 != -1) {

                            mismatches += m1.mismatch;
                            lenghts += n_dontcares;

                            for (int i2 = i1 + 1; i2 < matches.size(); i2++) {
                                m2 = matches.get(i2);

                                if (m2.pos1 == m1.pos1)
                                    m2.pos1 = -1;

                                if (m2.pos2 == m1.pos2)
                                    m2.pos2 = -1;
                            }

                            m1.pos1 = -1;
                            m1.pos2 = -1;
                        }
                    }

                    mismatches_list.add(new Tuple2<>(new SequencePair(id1, id2), new SwValue(mismatches, lenghts)));
                }
            }
        return mismatches_list;
    }
}
