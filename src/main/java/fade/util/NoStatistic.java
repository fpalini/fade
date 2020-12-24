package fade.util;

public class NoStatistic extends Statistic {
	private static NoStatistic noStatistic;

    private NoStatistic() { }

    public static NoStatistic getSingleton() {
        if (noStatistic== null)
            noStatistic = new NoStatistic();

        return noStatistic;
    }

    @Override
    public int getHashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoStatistic;
    }
}
