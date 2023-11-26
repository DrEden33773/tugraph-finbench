package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.ember.TuGraphFinbench.Algorithms.Case4Message;
import org.ember.TuGraphFinbench.Cell.Case4Cell;

import java.util.Map;

@Data
@AllArgsConstructor
public class Case4Vertex {
    VertexType vertexType;
    long ID;
    int highestLayer;
    Map<Long, Double> ahead0LayerLoanAmountMap;
    Map<Long, Double> ahead1LayerLoanAmountMap;
    Map<Long, Double> ahead2LayerLoanAmountMap;
    Map<Long, Double> ahead3LayerLoanAmountMap;

    public Case4Message toCase4Message() {
        return new Case4Message(
                highestLayer,
                ahead0LayerLoanAmountMap,
                ahead1LayerLoanAmountMap,
                ahead2LayerLoanAmountMap,
                ahead3LayerLoanAmountMap
        );
    }

    public double getHighestLayerLoanAmountSum() {
        double res = 0.0;
        if (highestLayer >= 1) {
            res += ahead1LayerLoanAmountMap.values().stream().mapToDouble(Double::doubleValue).sum();
        }
        if (highestLayer >= 2) {
            res += ahead2LayerLoanAmountMap.values().stream().mapToDouble(Double::doubleValue).sum();
        }
        if (highestLayer == 3) {
            res += ahead3LayerLoanAmountMap.values().stream().mapToDouble(Double::doubleValue).sum();
        }
        res /= 1e8;
        return res;
    }

    public Case4Cell toCase4Cell() {
        return new Case4Cell(ID, getHighestLayerLoanAmountSum());
    }
}
