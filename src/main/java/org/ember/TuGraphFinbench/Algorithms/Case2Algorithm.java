package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.model.common.Null;
import org.ember.TuGraphFinbench.Record.Case2Vertex;

public class Case2Algorithm extends VertexCentricCompute<Long, Case2Vertex, Null, Null> {
    public Case2Algorithm(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricCombineFunction<Null> getCombineFunction() {
        return null;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case2Vertex, Null, Null> getComputeFunction() {
        return null;
    }
}
