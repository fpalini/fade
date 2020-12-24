package fade.util;

import scala.Serializable;

import java.util.Arrays;
import java.util.Objects;

public class SequencePair implements Serializable {
    public final int id1, id2;

    public SequencePair(int id1, int id2) {
        this.id1 = Math.min(id1, id2);
        this.id2 = Math.max(id1, id2);;
    }

    public SequencePair(SequenceId seqId1, SequenceId seqId2) {
        this(seqId1.id, seqId2.id);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new int[]{id1, id2});
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof SequencePair)) return false;

        return ((SequencePair)o).id1 == id1 && ((SequencePair)o).id2 == id2;
    }

    @Override
    public String toString() {
        return "(" + id1 + ", " + id2 + ")";
    }
}