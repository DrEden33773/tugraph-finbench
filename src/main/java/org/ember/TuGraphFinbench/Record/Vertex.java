package org.ember.TuGraphFinbench.Record;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.ToString;

import org.ember.TuGraphFinbench.Util.IntoTrait;

@Data
@AllArgsConstructor
@ToString
public class Vertex implements IntoTrait<RawVertex> {
    long ID;
    long rawID;
    double loanAmount;

    @Override
    public RawVertex into() {
        byte tags = (byte) (ID & 0b11);
        VertexType nodeType = VertexType.fromByte(tags);
        return new RawVertex(nodeType, rawID, loanAmount);
    }
}
