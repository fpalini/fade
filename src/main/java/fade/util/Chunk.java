package fade.util;

import scala.Serializable;

public class Chunk implements Serializable {
    public final int id;
    public final long offset;
    public final byte[] data;

    public Chunk(int id, long offset, byte[] data) {
        this.id = id;
        this.offset = offset;
        this.data = data;
    }
}
