package org.ember.TuGraphFinbench.Record;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.ToString;

import org.ember.TuGraphFinbench.Util.IntoTrait;

@Data
@AllArgsConstructor
@ToString
public class RawEdge implements IntoTrait<Edge> {
    long src;
    VertexType srcType;
    long dst;
    VertexType dstType;
    double amount;

    @Override
    public Edge into() {
        long srcCodec = (src << 2) | srcType.toByte();
        long dstCodec = (dst << 2) | dstType.toByte();
        return new Edge(srcCodec, dstCodec, amount);
    }

    public Case1Edge toCase1Edge() {
        EdgeType edgeType = null;
        if (srcType == VertexType.Person && dstType == VertexType.Account) {
            edgeType = EdgeType.Own;
        }
        if (srcType == VertexType.Account && dstType == VertexType.Account) {
            edgeType = EdgeType.Transfer;
        }
        if (srcType == VertexType.Loan && dstType == VertexType.Account) {
            edgeType = EdgeType.Deposit;
        }
        return edgeType == null ? null : new Case1Edge(edgeType, src, dst);
    }

    public Case2Edge toCase2Edge() {
        if (srcType == VertexType.Account && dstType == VertexType.Account) {
            return new Case2Edge(src, dst);
        } else {
            return null;
        }
    }

    public Case3Edge toCase3Edge() {
        if (srcType == VertexType.Account && dstType == VertexType.Account) {
            return new Case3Edge(src, dst, amount);
        } else {
            return null;
        }
    }

    public Case4Edge toCase4Edge() {
        EdgeType edgeType = null;
        if (srcType == VertexType.Person && dstType == VertexType.Person) {
            edgeType = EdgeType.Guarantee;
        }
        if (srcType == VertexType.Person && dstType == VertexType.Loan) {
            edgeType = EdgeType.Apply;
        }
        return new Case4Edge(edgeType, src, dst);
    }
}
