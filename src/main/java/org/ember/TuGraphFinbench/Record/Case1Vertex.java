package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.ember.TuGraphFinbench.Cell.Case1Cell;

@Data
@AllArgsConstructor
public class Case1Vertex {
    VertexType vertexType;
    long ID;
    double loanAmountSum;
    /* MUST START WITH 0 */
    int nthLayer;

    public Case1Cell toCase1Cell() {
        return new Case1Cell(ID, loanAmountSum);
    }
}
