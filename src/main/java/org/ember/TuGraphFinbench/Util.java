package org.ember.TuGraphFinbench;

import org.ember.TuGraphFinbench.Record.VertexType;

public class Util {
    public static long globalID(VertexType t, long originID) {
        long flag;
        switch (t) {
            case Person:
                flag = 1;
                break;
            case Account:
                flag = 2;
                break;
            case Loan:
                flag = 3;
                break;
            default:
                flag = 0;
        }
        return (originID << 2) | flag;
    }
}
