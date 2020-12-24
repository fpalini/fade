package fade.util;

public class CountValue implements Value {
    public final long count;

    public CountValue(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "" + count;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CountValue)) return false;

        return ((CountValue)obj).count == count;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(count);
    }
}
