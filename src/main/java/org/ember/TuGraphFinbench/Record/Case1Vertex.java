package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class Case1Vertex {
    VertexType vertexType;
    long ID;
    double loanAmountSum;
    /* MUST START WITH 0 */
    int nthLayer;
}
