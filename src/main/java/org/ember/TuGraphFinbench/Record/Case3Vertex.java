package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.ember.TuGraphFinbench.Cell.Case3Cell;

@Data
@AllArgsConstructor
public class Case3Vertex {
    long ID;
    double inSum;
    double outSum;
    double inOutRatio;
    boolean hasIn;
    boolean hasOut;

    public Case3Cell toCase3Cell() {
        return new Case3Cell(ID, inOutRatio);
    }
}
