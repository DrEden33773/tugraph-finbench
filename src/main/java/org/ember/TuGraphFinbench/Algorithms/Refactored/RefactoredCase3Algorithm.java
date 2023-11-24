package org.ember.TuGraphFinbench.Algorithms.Refactored;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import org.ember.TuGraphFinbench.Record.Case3Vertex;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

public class RefactoredCase3Algorithm extends VertexCentricCompute<Long, Case3Vertex, Double, Null> {
    public RefactoredCase3Algorithm(long iterations) {
        super(iterations);
        assert iterations == 1;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case3Vertex, Double, Null> getComputeFunction() {
        return new RefactoredCase3ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Null> getCombineFunction() {
        return null;
    }

    public static class RefactoredCase3ComputeFunction extends AbstractVcFunc<Long, Case3Vertex, Double, Null> {
        @Override
        public void compute(Long vertexId, Iterator<Null> _) {
            final Case3Vertex currVertex = this.context.vertex().get().getValue();
            final List<IEdge<Long, Double>> inEdges = this.context.edges().getInEdges();
            if (!inEdges.isEmpty()) {
                currVertex.setHasIn(true);
            }
            double inSum = 0.0;
            for (IEdge<Long, Double> edge : inEdges) {
                inSum += edge.getValue();
            }
            currVertex.setInSum(inSum);
            final List<IEdge<Long, Double>> outEdges = this.context.edges().getOutEdges();
            if (!outEdges.isEmpty()) {
                currVertex.setHasOut(true);
            }
            double outSum = 0.0;
            for (IEdge<Long, Double> edge : outEdges) {
                outSum += edge.getValue();
            }
            currVertex.setOutSum(outSum);
            if (inSum == 0.0 || outSum == 0.0) {
                return;
            }
            double res = inSum / outSum;
            final DecimalFormat dFormat = new DecimalFormat("#.00");
            res = Double.parseDouble(dFormat.format(res));
            currVertex.setInOutRatio(res);
        }
    }
}
