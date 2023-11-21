package org.ember.TuGraphFinbench.Record;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class Case1Edge {
    EdgeType edgeType;
    long srcID;
    long dstID;
}
