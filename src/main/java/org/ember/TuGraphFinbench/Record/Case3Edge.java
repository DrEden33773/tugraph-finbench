package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Case3Edge {
    long srcID;
    long dstID;
    double transferAmount;
}
