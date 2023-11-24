package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Case1Vertex {
    VertexType vertexType;
    long ID;
    double loanAmountSum;
    /* MUST START WITH 0 */
    int nthLayer;
}
