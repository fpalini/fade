package fade.mash;

import fade.util.Value;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;

public class MashValue implements Value {
    public final long[] hashes;

    public MashValue(long[] hashes) {
        this.hashes = hashes;
    }
}
