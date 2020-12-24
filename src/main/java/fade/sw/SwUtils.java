package fade.sw;

public class SwUtils {
    static final int[][][] Chiaromonte = new int[4][][];
    static final byte[][][] mismatches = new byte[4][][];

    static {

        int[][] Chiaromonte1 = {
                {   91, -114,  -31, -123 },
                { -114,  100, -125,  -31 },
                {  -31, -125,  100, -114 },
                { -123,  -31, -114,   91 }
        };

        byte[][] mismatches1 = new byte[4][4]; // 16B

        int[][] Chiaromonte2 = new int[16][16];  // 1KB
        byte[][] mismatches2 = new byte[16][16]; // 256B

        int[][] Chiaromonte3 = new int[64][64];  // 16KB
        byte[][] mismatches3 = new byte[64][64]; // 4KB

        int[][] Chiaromonte4 = new int[256][256];  // 256KB
        byte[][] mismatches4 = new byte[256][256]; // 64KB

        byte[] nucleotides = {0, 1, 2, 3};
        int pair1, pair2, triplet1, triplet2, quartet1, quartet2;

        for (byte n1 : nucleotides)
            for (byte n2 : nucleotides) {
                mismatches1[n1][n2] = (byte) (n1 != n2? 1 : 0);

                for (byte n3 : nucleotides)
                    for (byte n4 : nucleotides) {

                        pair1 = pair2byte(n1, n3) & 0xFF;
                        pair2 = pair2byte(n2, n4) & 0xFF;

                        Chiaromonte2[pair1][pair2] = Chiaromonte1[n1][n2] + Chiaromonte1[n3][n4];
                        mismatches2[pair1][pair2] = (byte) (mismatches1[n1][n2] + (n3 != n4? 1 : 0));

                        for (byte n5 : nucleotides)
                            for (byte n6 : nucleotides) {

                                triplet1 = triplet2byte(n1, n3, n5) & 0xFF;
                                triplet2 = triplet2byte(n2, n4, n6) & 0xFF;

                                Chiaromonte3[triplet1][triplet2] = Chiaromonte2[pair1][pair2] + Chiaromonte1[n5][n6];
                                mismatches3[triplet1][triplet2] = (byte) (mismatches2[pair1][pair2] + (n5 != n6? 1 : 0));

                                for (byte n7 : nucleotides)
                                    for (byte n8 : nucleotides) {

                                        quartet1 = quartet2byte(n1, n3, n5, n7) & 0xFF;
                                        quartet2 = quartet2byte(n2, n4, n6, n8) & 0xFF;

                                        Chiaromonte4[quartet1][quartet2] = Chiaromonte3[triplet1][triplet2] + Chiaromonte1[n7][n8];
                                        mismatches4[quartet1][quartet2] = (byte) (mismatches3[triplet1][triplet2] + (n7 != n8? 1 : 0));
                                    }
                            }
                    }
            }

        Chiaromonte[0] = Chiaromonte4;
        Chiaromonte[1] = Chiaromonte1;
        Chiaromonte[2] = Chiaromonte2;
        Chiaromonte[3] = Chiaromonte3;

        mismatches[0] = mismatches4;
        mismatches[1] = mismatches1;
        mismatches[2] = mismatches2;
        mismatches[3] = mismatches3;
    }

    static byte pair2byte(byte n1, byte n2) {
        byte b = (byte) (n1 << 2);
        b |= n2;

        return b;
    }

    static byte triplet2byte(byte n1, byte n2, byte n3) {
        byte b = (byte) (n1 << 4);
        b |= n2 << 2;
        b |= n3;

        return b;
    }

    static byte quartet2byte(byte n1, byte n2, byte n3, byte n4) {
        byte b = (byte) (n1 << 6);
        b |= n2 << 4;
        b |= n3 << 2;
        b |= n4;

        return b;
    }
}
