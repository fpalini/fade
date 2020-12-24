package fade.mash;

import fade.util.Data;

public class MashData implements Data {

    public final long[] sketches;

    public MashData(long[] sketches) {
        this.sketches = sketches;
    }

    @Override
    public boolean filter(String s) {
        return true;
    }
}
