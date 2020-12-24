package fade.mash;

import fade.util.Statistic;

public class HashCode extends Statistic {
	public final long hashcode;

    public HashCode(long hashcode) {
        this.hashcode = hashcode;
    }

	@Override
	public int getHashCode() {
		return Long.hashCode(hashcode);
	}
	
	public boolean equals(Object obj) {
        if (!(obj instanceof HashCode)) return false;

        return (((HashCode)obj).hashcode == hashcode);
    }


}
