package org.ember.TuGraphFinbench.Algorithms;

import java.util.Iterator;

import org.ember.TuGraphFinbench.Record.Node;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;

public class Case3 extends VertexCentricCompute<Long, Node, Double, Node> {

    public Case3(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricComputeFunction<Long, Node, Double, Node> getComputeFunction() {
        return new Case3ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Node> getCombineFunction() {
        return null;
    }

    public class Case3ComputeFunction extends AbstractVcFunc<Long, Node, Double, Node> {

        @Override
        public void compute(Long vertexId, Iterator<Node> messageIterator) {
        }

    }
}
