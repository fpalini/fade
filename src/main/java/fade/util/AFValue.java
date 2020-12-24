package fade.util;

import scala.Serializable;

public class AFValue implements Serializable {
    public final double value;

    public AFValue(double value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AFValue)) return false;

        return ((AFValue)obj).value == value;
    }

    @Override
    public String toString() {
        return value + "";
    }
}