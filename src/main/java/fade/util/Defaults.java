package fade.util;

public abstract class Defaults {
    // Distance
    public static final boolean LOCAL = true;
    public static final Strategy STRATEGY = Strategy.PARTIAL_AGGREGATION;
    public static final String ASSEMBLED = "long";
    public static final String TYPE = "fasta";
    public static final int BUFFER_SIZE = 4096;
    public static final int SLICES = 128;
    public static final int SPLIT_SIZE = 100 * 1000;

    // Simulation
    public static final String SIM_DISTANCES_PATH = "simulation_bkp/distances/dist";
    public static final String SIM_RANDOMS_PATH = "simulation_bkp/randoms/rnd";
    public static final int Q = 10;
    public static final int SIMULATIONS = 1;
    public static final double ALPHA = 0.1;
    public static final boolean SIGNIFICANCE = true;
    public static final boolean BIN_SIZES = false;
    public static final boolean CANONICAL = true;
}
