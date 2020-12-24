package fade.simulation;

import scala.Serializable;

public class Rank implements Serializable {
    public int value;

    public Rank(int value) {
        this.value = value;
    }

    public void higher() {
        value--;
    }

    public void lower() {
        value++;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Rank)) return false;

        return ((Rank)o).value == value;
    }

    @Override
    public String toString() {
        return value + "";
    }
}
