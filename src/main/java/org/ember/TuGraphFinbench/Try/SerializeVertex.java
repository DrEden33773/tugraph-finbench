package org.ember.TuGraphFinbench.Try;

import org.ember.TuGraphFinbench.Record.RawVertex;
import org.ember.TuGraphFinbench.Record.VertexType;
import org.ember.TuGraphFinbench.Record.Vertex;

public class SerializeVertex {
    public static void main(String[] args) {
        RawVertex nodeRecord = new RawVertex(VertexType.Account, 123456, 0);
        System.out.println(nodeRecord);
        Vertex node = nodeRecord.into();
        System.out.println(node);
        RawVertex nodeRecord2 = node.into();
        System.out.println(nodeRecord2);
    }
}
