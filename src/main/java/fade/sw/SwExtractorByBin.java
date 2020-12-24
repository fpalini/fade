package fade.sw;

import fade.util.*;
import fade.StatisticExtractorByBin;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SwExtractorByBin extends StatisticExtractorByBin {

    private final Integer[] onesPosition, dontcaresPosition;
    private final int n_dontcares, n_dontcares_mod, n_dontcares_div;
    private final int w;

    public SwExtractorByBin(Configuration conf) {
        super(conf);
        byte[] pattern_array = conf.get("pattern").getBytes();

        ArrayList<Integer> onesPosition_list = new ArrayList<>();
        ArrayList<Integer> dontcaresPosition_list = new ArrayList<>();

        for (int i = 0; i < pattern_array.length; i++)
            if (pattern_array[i] == '1')
                onesPosition_list.add(i);
            else
                dontcaresPosition_list.add(i);

        int l = pattern_array.length;
        w = onesPosition_list.size();

        onesPosition = onesPosition_list.toArray(new Integer[0]);
        dontcaresPosition = dontcaresPosition_list.toArray(new Integer[0]);

        n_dontcares = l-w;
        n_dontcares_mod = n_dontcares % 4;
        n_dontcares_div = (int) Math.ceil(n_dontcares / 4.0);
    }

    @Override
    public List<Tuple2<BinId, Tuple2<SequenceId, Data>>> call(Chunk chunk) throws Exception {
        int l = w + n_dontcares;
        int len = chunk.data.length;
        int W;
        byte [] dontcares;

        ArrayList<Tuple2<BinId, Tuple2<SequenceId, Data>>> spacedWords = new ArrayList<>(len - (l-1));

        for (int i = 0; i < len; i++)
            chunk.data[i] = KmerTool.bitmask[chunk.data[i]];

        for (int pos = 0; pos < len - (l-1); pos++) {
            W = 0;

            for (int i = 0; i < w; i++) {
                W <<= 2;
                W += chunk.data[pos + onesPosition[i]];
            }

            dontcares = new byte[n_dontcares_div];
            int i;

            for (i = 0; i < n_dontcares - n_dontcares_mod; i+=4)
                dontcares[i/4] = SwUtils.quartet2byte(
                        chunk.data[pos + dontcaresPosition[i]],
                        chunk.data[pos + dontcaresPosition[i+1]],
                        chunk.data[pos + dontcaresPosition[i+2]],
                        chunk.data[pos + dontcaresPosition[i+3]]
                );

            switch(n_dontcares_mod) {
                case 0: break;

                case 1: dontcares[i/4] = chunk.data[pos + dontcaresPosition[i]];
                break;

                case 2: dontcares[i/4] = SwUtils.pair2byte(
                        chunk.data[pos + dontcaresPosition[i]],
                        chunk.data[pos + dontcaresPosition[i+1]]
                ); break;

                case 3: dontcares[i/4] = SwUtils.triplet2byte(
                        chunk.data[pos + dontcaresPosition[i]],
                        chunk.data[pos + dontcaresPosition[i+1]],
                        chunk.data[pos + dontcaresPosition[i+2]]
                ); break;

                default: throw new Exception("Wrong number of bytes exceeded: "+ n_dontcares_mod);
            }

            spacedWords.add(new Tuple2<>(new BinId(W), new Tuple2<>(new SequenceId(chunk.id), new SwData(chunk.offset + pos, dontcares))));
        }

        if(!getConf().getString("assembled", Defaults.ASSEMBLED).equals("no")) {
            // Reverse Complementary
            long seqLen = getConf().getLong("len_s" + chunk.id);
            int n = getConf().getInt("n");

            byte[] idRevMap = new byte[4];
            idRevMap[0] = 3;
            idRevMap[1] = 2;
            idRevMap[2] = 1;
            idRevMap[3] = 0;

            for (int i = 0; i < len; i++)
                chunk.data[i] = idRevMap[chunk.data[i]];

            for (int pos = l-1; pos < len; pos++) {
                W = 0;

                for (int i = 0; i < w; i++) {
                    W <<= 2;
                    W += chunk.data[pos - onesPosition[i]];
                }

                dontcares = new byte[n_dontcares_div];
                int i;

                for (i = 0; i < n_dontcares - n_dontcares_mod; i+=4)
                    dontcares[i/4] = SwUtils.quartet2byte(
                            chunk.data[pos - dontcaresPosition[i]],
                            chunk.data[pos - dontcaresPosition[i+1]],
                            chunk.data[pos - dontcaresPosition[i+2]],
                            chunk.data[pos - dontcaresPosition[i+3]]
                    );

                switch(n_dontcares_mod) {
                    case 0: break;

                    case 1: dontcares[i/4] = chunk.data[pos - dontcaresPosition[i]];
                        break;

                    case 2: dontcares[i/4] = SwUtils.pair2byte(
                            chunk.data[pos - dontcaresPosition[i]],
                            chunk.data[pos - dontcaresPosition[i+1]]
                    ); break;

                    case 3: dontcares[i/4] = SwUtils.triplet2byte(
                            chunk.data[pos - dontcaresPosition[i]],
                            chunk.data[pos - dontcaresPosition[i+1]],
                            chunk.data[pos - dontcaresPosition[i+2]]
                    ); break;

                    default: throw new Exception("Wrong number of bytes exceeded: "+ n_dontcares_mod);
                }

                spacedWords.add(new Tuple2<>(new BinId(W), new Tuple2<>(new SequenceId(n+chunk.id), new SwData((seqLen-1) - (chunk.offset + pos), dontcares))));
            }
        }

        return spacedWords;
    }
}
