package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.MutablePair;
import org.ember.TuGraphFinbench.Cell.Case2Cell;

import java.util.List;

@Data
@AllArgsConstructor
public class Case2Vertex {
    long ID;
    long ringCount;
    List<MutablePair<Long, Long>> prevAncestors;

    public Case2Cell toCase2Cell() {
        return new Case2Cell(ID, ringCount);
    }
}
