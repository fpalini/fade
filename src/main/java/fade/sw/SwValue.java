package fade.sw;

import fade.util.Value;

public class SwValue implements Value {
    public final long mismatches, dontcares;

    public SwValue(long mismatches, long dontcares) {
        this.mismatches = mismatches;
        this.dontcares = dontcares;
    }

    @Override
    public String toString() {
        return "(" + mismatches + "," + dontcares + ")";
    }
}
