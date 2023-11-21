package org.ember.TuGraphFinbench.Record;

import lombok.Data;
import lombok.ToString;
import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
@ToString
public class Edge {
    long srcCodec;
    long dstCodec;
    double amount;
}
