package fade.util;

import scala.Serializable;

public class Id implements Serializable {
    public final int id;

    public Id(int id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Id)) return false;

        return ((Id)o).id == id;
    }

    @Override
    public String toString() {
        return id + "";
    }
}
