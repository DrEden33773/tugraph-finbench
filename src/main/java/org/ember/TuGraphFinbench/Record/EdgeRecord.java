package org.ember.TuGraphFinbench.Record;

import lombok.Data;

import org.ember.TuGraphFinbench.Util.IntoTrait;

import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
public class EdgeRecord implements IntoTrait<Edge> {
    long src;
    NodeType srcType;
    long dst;
    NodeType dstType;
    double amount;

    @Override
    public Edge into() {
        long srcCodec = (src << 2) | srcType.toByte();
        long dstCodec = (dst << 2) | dstType.toByte();
        return new Edge(srcCodec, dstCodec, amount);
    }
}
