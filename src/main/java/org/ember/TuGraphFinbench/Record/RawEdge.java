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
}
