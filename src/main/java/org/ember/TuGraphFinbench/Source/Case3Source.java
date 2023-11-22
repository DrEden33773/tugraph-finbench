package org.ember.TuGraphFinbench.Source;

import org.ember.TuGraphFinbench.Record.Case3Vertex;

import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

public class Case3Source {

    public static class Case3VertexSource extends BaseSource<IVertex<Long, Case3Vertex>> {
        @Override
        public void init(int parallel, int index) {
            throw new UnsupportedOperationException("Unimplemented method 'init'");
        }
    }

    public static class Case3EdgeSource extends BaseSource<IEdge<Long, Double>> {
        @Override
        public void init(int parallel, int index) {
            throw new UnsupportedOperationException("Unimplemented method 'init'");
        }
    }
}
