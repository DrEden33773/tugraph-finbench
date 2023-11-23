package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.orc.util.Murmur3;
import org.ember.TuGraphFinbench.Util.IntoTrait;

import java.util.ArrayList;

@Data
@AllArgsConstructor
@ToString
public class RawVertex implements IntoTrait<Vertex> {
    VertexType vertexType;
    long rawID;
    double loanAmount;

    @Override
    public Vertex into() {
        byte tags = vertexType.toByte();
        return new Vertex((Murmur3.hash64(Long.valueOf(rawID).toString().getBytes()) << 2) | tags, rawID, loanAmount);
    }

    public Case1Vertex toCase1Vertex() {
        return new Case1Vertex(vertexType, rawID, 0.0);
    }

    public Case2Vertex toCase2Vertex() {
        if (vertexType != VertexType.Account) {
            return null;
        }
        return new Case2Vertex(rawID, 0, new ArrayList<>());
    }

    public Case3Vertex toCase3Vertex() {
        if (vertexType != VertexType.Account) {
            return null;
        }
        return new Case3Vertex(rawID, 0.0, 0.0, 0.0, false, false);
    }

    public Case4Vertex toCase4Vertex() {
        if (vertexType == VertexType.Account) {
            return null;
        }
        return new Case4Vertex(vertexType, rawID, 0.0);
    }
}
