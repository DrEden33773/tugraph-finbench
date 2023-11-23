package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import org.ember.TuGraphFinbench.Record.Case3Vertex;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

public class Case3Algorithm extends VertexCentricCompute<Long, Case3Vertex, Double, Double> {

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
        public void compute(Long vertexId, Iterator<Double> messageIterator) {
            Case3Vertex currVertex = this.context.vertex().get().getValue();
            List<IEdge<Long, Double>> edges = this.context.edges().getOutEdges();
            if (!edges.isEmpty()) {
                currVertex.setHasOut(true);
            }

            double inSum = 0.0, outSum = 0.0;

            if (this.context.getIterationId() == 1) {
                for (IEdge<Long, Double> edge : edges) {
                    this.context.sendMessageToNeighbors(edge.getValue());
                    outSum += edge.getValue();
                }
                return;
            }

            if (messageIterator.hasNext()) {
                currVertex.setHasIn(true);
            }
            while (messageIterator.hasNext()) {
                inSum += messageIterator.next();
            }
            if (inSum == 0.0 || outSum == 0.0) {
                return;
            }

            double res = inSum / outSum;
            DecimalFormat dFormat = new DecimalFormat("#.00");
            res = Double.parseDouble(dFormat.format(res));

            currVertex.setInOutRatio(res);
        }
    }
}
