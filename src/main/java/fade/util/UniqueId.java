package fade.util;

public class UniqueId extends BinId {
    private static UniqueId id;

    private UniqueId(int id) {
        super(id);
    }

    public static UniqueId getInstance() {
        if (id == null)
            id = new UniqueId(0);

        return id;
    }
}
