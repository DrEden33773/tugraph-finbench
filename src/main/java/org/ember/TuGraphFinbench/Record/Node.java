package org.ember.TuGraphFinbench.Record;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.ToString;

import org.ember.TuGraphFinbench.Util.IntoTrait;

@Data
@AllArgsConstructor
@ToString
public class Node implements IntoTrait<NodeRecord> {
    long ID;
    long rawID;
    long loanAmount;

    @Override
    public NodeRecord into() {
        byte tags = (byte) (ID & 0b11);
        NodeType nodeType = NodeType.fromByte(tags);
        return new NodeRecord(nodeType, rawID, loanAmount);
    }

}
