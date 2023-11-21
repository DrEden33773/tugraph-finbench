package org.ember.TuGraphFinbench.Algorithms;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.List;

import org.ember.TuGraphFinbench.Record.Case3Vertex;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.graph.edge.IEdge;

public class Case3Algorithm extends VertexCentricCompute<Long, Case3Vertex, Double, Double> {

    public Case3Algorithm(long iterations) {
        super(iterations);
        assert iterations == 2L;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case3Vertex, Double, Double> getComputeFunction() {
        return new Case3ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Double> getCombineFunction() {
        return null;
    }

    public class Case3ComputeFunction extends AbstractVcFunc<Long, Case3Vertex, Double, Double> {

        @Override
        public void compute(Long vertexId, Iterator<Double> messageIterator) {
            Case3Vertex currVertex = this.context.vertex().get().getValue();

            List<IEdge<Long, Double>> edges = this.context.edges().getOutEdges();
            if (this.context.getIterationId() == 1L) {
                for (IEdge<Long, Double> edge : edges) {
                    this.context.sendMessage(edge.getTargetId(), edge.getValue());
                }
                return;
            }

            double inSum = 0.0, outSum = 0.0;

            while (messageIterator.hasNext()) {
                inSum += messageIterator.next();
            }
            if (inSum == 0.0) {
                return;
            }

            for (IEdge<Long, Double> edge : edges) {
                outSum += edge.getValue();
            }
            if (outSum == 0.0) {
                return;
            }

            double res = inSum / outSum;
            BigDecimal bigDecimal = new BigDecimal(res);
            // bigDecimal = res < 1.0 ? bigDecimal.round(new MathContext(2,
            // RoundingMode.DOWN))
            // : bigDecimal.round(new MathContext(2, RoundingMode.HALF_UP));
            bigDecimal = bigDecimal.round(new MathContext(2, RoundingMode.HALF_UP));
            res = bigDecimal.doubleValue();

            this.context.setNewVertexValue(new Case3Vertex(currVertex.getID(), res));
        }
    }
}
