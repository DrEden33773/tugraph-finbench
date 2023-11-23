package org.ember.TuGraphFinbench.Env;

public class Env {
    static final boolean PARALLEL = true;
    public static final int PARALLELISM_MAX = PARALLEL ? GET_PARALLELISM_MAX() : 1;

    static int GET_PARALLELISM_MAX() {
        final int PARALLELISM_MAX = Runtime.getRuntime().availableProcessors();
        int parallelismMax = Integer.highestOneBit(PARALLELISM_MAX);
        if (parallelismMax > PARALLELISM_MAX) {
            parallelismMax >>= 1;
        }
        return parallelismMax;
    }
}
