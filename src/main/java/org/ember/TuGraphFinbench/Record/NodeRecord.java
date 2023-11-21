package org.ember.TuGraphFinbench.Record;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.ToString;

import org.apache.orc.util.Murmur3;
import org.ember.TuGraphFinbench.Util.IntoTrait;

@Data
@AllArgsConstructor
@ToString
public class NodeRecord implements IntoTrait<Node> {
    NodeType nodeType;
    long rawID;
    long loanAmount;

    @Override
    public Node into() {
        byte tags = nodeType.toByte();
        return new Node((Murmur3.hash64(Long.valueOf(rawID).toString().getBytes()) << 2) | tags, rawID, loanAmount);
    }

}
