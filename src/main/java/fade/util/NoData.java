package fade.util;

public class NoData implements Data {
    private static NoData noData;

    private NoData() { }

    public static NoData getInstance() {
        if (noData == null)
            noData = new NoData();

        return noData;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoStatistic;
    }
}
