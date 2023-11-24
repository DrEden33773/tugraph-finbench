package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Case4Edge {
    EdgeType edgeType;
    long srcID;
    long dstID;
}
