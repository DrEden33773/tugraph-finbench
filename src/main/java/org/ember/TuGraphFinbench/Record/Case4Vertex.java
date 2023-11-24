package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Case4Vertex {
    VertexType vertexType;
    long ID;
    double loanAmountSum;
}
