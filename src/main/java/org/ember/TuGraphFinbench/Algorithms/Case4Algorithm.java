package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import org.ember.TuGraphFinbench.Record.Case4Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;

import java.util.Iterator;


public class Case4Algorithm extends VertexCentricCompute<Long, Case4Vertex, Null, Double> {
    public Case4Algorithm(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricComputeFunction<Long, Case4Vertex, Null, Double> getComputeFunction() {
        return new Case1ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Double> getCombineFunction() {
        return null;
    }

    public static class Case1ComputeFunction extends AbstractVcFunc<Long, Case4Vertex, Null, Double> {
        @Override
        public void compute(final Long vertexId, final Iterator<Double> messageIterator) {
            switch ((int) this.context.getIterationId()) {
                case 1:
                    computeIter1(vertexId);
                    break;
                default:
                    throw new RuntimeException("Invalid iteration id: " + this.context.getIterationId());
            }
        }

        void computeIter1(final Long vertexId) {
            final Case4Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Loan) {
                return;
            }
            this.context.sendMessageToNeighbors(currVertex.getLoanAmountSum());
        }
    }
}
