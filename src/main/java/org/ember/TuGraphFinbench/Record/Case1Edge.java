package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Case1Edge {
    EdgeType edgeType;
    long srcID;
    long dstID;
}
