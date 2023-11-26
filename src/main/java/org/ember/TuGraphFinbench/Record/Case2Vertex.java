package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.ember.TuGraphFinbench.Cell.Case2Cell;

@Data
@AllArgsConstructor
public class Case2Vertex {
    long ID;
    long ringCount;

    public Case2Cell toCase2Cell() {
        return new Case2Cell(ID, ringCount);
    }
}
