package fade.util;

import scala.Serializable;

public interface Data extends Serializable {
    default long size() { return 1; }
    default boolean filter(String regex) { return true; }
}