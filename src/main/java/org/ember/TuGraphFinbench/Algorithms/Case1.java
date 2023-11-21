package org.ember.TuGraphFinbench.Algorithms;

import java.util.Iterator;

import org.ember.TuGraphFinbench.Record.Node;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;

public class Case1 extends VertexCentricCompute<Long, Node, Double, Node> {

    public Case1(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricComputeFunction<Long, Node, Double, Node> getComputeFunction() {
        return new Case1ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Node> getCombineFunction() {
        return null;
    }

    public class Case1ComputeFunction extends AbstractVcFunc<Long, Node, Double, Node> {

        @Override
        public void compute(Long vertexId, Iterator<Node> messageIterator) {
            throw new UnsupportedOperationException("Unimplemented method 'compute'");
        }

    }
}
