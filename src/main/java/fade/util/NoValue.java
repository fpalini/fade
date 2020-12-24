package fade.util;

public class NoValue implements Value {
	private static NoValue noValue;

    private NoValue() { }

    public static NoValue getSingleton() {
        if (noValue== null)
            noValue = new NoValue();

        return noValue;
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
