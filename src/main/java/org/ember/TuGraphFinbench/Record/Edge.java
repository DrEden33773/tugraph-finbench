package org.ember.TuGraphFinbench.Record;

import lombok.Data;
import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
public class Edge {
    long srcCodec;
    long dstCodec;
    double amount;
}
