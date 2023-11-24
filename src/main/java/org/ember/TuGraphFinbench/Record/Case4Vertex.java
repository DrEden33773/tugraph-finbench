package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class Case4Vertex {
    VertexType vertexType;
    long ID;
    double loanAmountSum;
}
