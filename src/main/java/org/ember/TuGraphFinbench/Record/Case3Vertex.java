package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Case3Vertex {
    long ID;
    double inSum;
    double outSum;
    double inOutRatio;
    boolean hasIn;
    boolean hasOut;
}
