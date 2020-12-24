package fade.sw;

import fade.util.Data;

public class SwData implements Data {
    long offset;
    byte[] dontcares;

    SwData(long offset, byte[] dontcares) {
        this.offset = offset;
        this.dontcares = dontcares;
    }
}
