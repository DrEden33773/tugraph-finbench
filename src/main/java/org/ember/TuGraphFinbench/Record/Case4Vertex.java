package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.ember.TuGraphFinbench.Algorithms.Case4Message;
import org.ember.TuGraphFinbench.Cell.Case4Cell;

@Data
@AllArgsConstructor
public class Case4Vertex {
    VertexType vertexType;
    long ID;
    double layer0LoanAmountSum;
    double layer1LoanAmountSum;
    double layer2LoanAmountSum;
    double layer3LoanAmountSum;
    int highestLayer;

    public Case4Message toCase4Message() {
        return new Case4Message(
                layer0LoanAmountSum,
                layer1LoanAmountSum,
                layer2LoanAmountSum,
                layer3LoanAmountSum,
                highestLayer
        );
    }

    public double getHighestLayerLoanAmountSum() {
        switch (highestLayer) {
            case 0:
                return layer0LoanAmountSum;
            case 1:
                return layer1LoanAmountSum;
            case 2:
                return layer2LoanAmountSum;
            case 3:
                return layer3LoanAmountSum;
            default:
                return 0;
        }
    }

    public Case4Cell toCase4Cell() {
        return new Case4Cell(ID, getHighestLayerLoanAmountSum());
    }
}
