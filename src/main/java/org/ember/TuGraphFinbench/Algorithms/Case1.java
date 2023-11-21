package org.ember.TuGraphFinbench.Algorithms;

import java.util.Iterator;

import org.ember.TuGraphFinbench.Record.Vertex;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;

public class Case1 extends VertexCentricCompute<Long, Vertex, Double, Vertex> {

    public Case1(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricComputeFunction<Long, Vertex, Double, Vertex> getComputeFunction() {
        return new Case1ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Vertex> getCombineFunction() {
        return null;
    }

    public class Case1ComputeFunction extends AbstractVcFunc<Long, Vertex, Double, Vertex> {

        @Override
        public void compute(Long vertexId, Iterator<Vertex> messageIterator) {
            throw new UnsupportedOperationException("Unimplemented method 'compute'");
        }

    }
}
