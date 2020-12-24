package fade.sw;

import scala.Serializable;

public class Match implements Serializable {

	public long pos1, pos2;
	public int mismatch, score;
	
	public Match(long pos1, long pos2, int mismatch, int score) {
		this.pos1 = pos1;
		this.pos2 = pos2;
		this.mismatch = mismatch;
		this.score = score;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Match)) return false;

		Match m = (Match) o;

		return m.pos1 == pos1 && m.pos2 == pos2 && m.mismatch == mismatch && m.score == score;
	}
}
