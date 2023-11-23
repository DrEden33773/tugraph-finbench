package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class Case3Vertex {
    long ID;
    double inSum;
    double outSum;
    double inOutRatio;
    boolean hasIn;
    boolean hasOut;
}
