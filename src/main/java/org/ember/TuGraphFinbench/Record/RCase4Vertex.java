package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.ember.TuGraphFinbench.Cell.Case4Cell;

import java.util.Map;

@Data
@AllArgsConstructor
public class RCase4Vertex {
    VertexType vertexType;
    long ID;
    double selfLoanAmountSum;
    Map<Long, Double> receivedPersonLoanAmountMap;

    public Case4Cell toCase4Cell() {
        return new Case4Cell(ID, receivedPersonLoanAmountMap.values().stream().mapToDouble(Double::doubleValue).sum() / 1e8);
    }
}
