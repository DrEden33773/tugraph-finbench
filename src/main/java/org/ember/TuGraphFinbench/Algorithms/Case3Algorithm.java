package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import org.ember.TuGraphFinbench.Record.Case3Vertex;

import java.util.Iterator;

public class Case3Algorithm extends VertexCentricCompute<Long, Case3Vertex, Double, Double> {

    public Case3Algorithm() {
        super(2);
    }

    public Case3Algorithm(long iterations) {
        super(iterations);
        assert iterations == 2;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case3Vertex, Double, Double> getComputeFunction() {
        return new Case3ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Double> getCombineFunction() {
        return null;
    }

    public static class Case3ComputeFunction extends AbstractVcFunc<Long, Case3Vertex, Double, Double> {

        @Override
        public void compute(final Long vertexId, final Iterator<Double> messageIterator) {
            final Case3Vertex currVertex = this.context.vertex().get().getValue();

            double inSum = currVertex.getInSum(), outSum = currVertex.getOutSum();

            if (this.context.getIterationId() == 1) {
                final Iterator<IEdge<Long, Double>> edges = this.context.edges().getOutEdges().iterator();
                if (edges.hasNext()) {
                    currVertex.setHasOut(true);
                }
                while (edges.hasNext()) {
                    final IEdge<Long, Double> edge = edges.next();
                    this.context.sendMessage(edge.getTargetId(), edge.getValue());
                    //this.context.sendMessageToNeighbors(edge.getValue());
                    outSum += edge.getValue();
                    currVertex.setOutSum(outSum);
                }
                return;
            }

            if (messageIterator.hasNext()) {
                currVertex.setHasIn(true);
            }
            while (messageIterator.hasNext()) {
                inSum += messageIterator.next();
            }
            currVertex.setInSum(inSum);
            if (inSum == 0.0 || outSum == 0.0) {
                return;
            }

            double res = inSum / outSum;

            // deprecated -> use String.format("%.2f", res) instead
            // final DecimalFormat dFormat = new DecimalFormat("#.00");
            // res = Double.parseDouble(dFormat.format(res));

            currVertex.setInOutRatio(res);
        }
    }
}
