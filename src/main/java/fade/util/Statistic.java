package fade.util;

import scala.Serializable;

public abstract class Statistic implements Serializable {

    public abstract int getHashCode();

    @Override
    public int hashCode() {
        return getHashCode();
    }

    public boolean filter(String regex) {
        return true;
    }
}
