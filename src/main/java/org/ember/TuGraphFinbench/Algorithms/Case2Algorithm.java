package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import org.apache.commons.lang3.tuple.MutablePair;
import org.ember.TuGraphFinbench.Record.Case2Vertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Case2Algorithm extends VertexCentricCompute<Long, Case2Vertex, Null, List<MutablePair<Long, Long>>> {
    public Case2Algorithm(long iterations) {
        super(iterations);
        assert iterations == 4;
    }

    @Override
    public VertexCentricCombineFunction<List<MutablePair<Long, Long>>> getCombineFunction() {
        return null;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case2Vertex, Null, List<MutablePair<Long, Long>>> getComputeFunction() {
        return new Case2ComputeFunction();
    }

    public static class Case2ComputeFunction extends AbstractVcFunc<Long, Case2Vertex, Null, List<MutablePair<Long, Long>>> {
        @Override
        public void compute(final Long vertexId, final Iterator<List<MutablePair<Long, Long>>> messageIterator) {
            switch ((int) this.context.getIterationId()) {
                case 1:
                    computeIter1(vertexId);
                    break;
                case 2:
                    computeIter2(vertexId, messageIterator);
                    break;
                case 3:
                    computeIter3(vertexId, messageIterator);
                    break;
                case 4:
                    computeIter4(vertexId, messageIterator);
                    break;
                default:
                    throw new RuntimeException("Invalid iteration id: " + this.context.getIterationId());
            }
        }

        void computeIter1(final Long vertexId) {
            List<MutablePair<Long, Long>> toSend = new ArrayList<>(1);
            toSend.add(new MutablePair<>(vertexId, vertexId));
            this.context.sendMessageToNeighbors(toSend);
        }

        void computeIter2(final Long vertexId, final Iterator<List<MutablePair<Long, Long>>> messageIterator) {
            Case2Vertex currentVertex = this.context.vertex().get().getValue();
            // update currentVertex's prevAncestors
            messageIterator.forEachRemaining(currentVertex.getPrevAncestors()::addAll);
            // wrap prevAncestors to send (prevID := currVertexID, ancestorID := currVertex.foreach.ancestorID)
            List<MutablePair<Long, Long>> toSend = new ArrayList<>(currentVertex.getPrevAncestors().size());
            currentVertex.getPrevAncestors().forEach(prevAncestorPair -> {
                MutablePair<Long, Long> toAdd = new MutablePair<>(vertexId, prevAncestorPair.getRight());
                toSend.add(toAdd);
            });
            this.context.sendMessageToNeighbors(toSend);
        }

        void computeIter3(final Long vertexId, final Iterator<List<MutablePair<Long, Long>>> messageIterator) {
            // same as iter2
            computeIter2(vertexId, messageIterator);
        }

        void computeIter4(final Long vertexId, final Iterator<List<MutablePair<Long, Long>>> messageIterator) {
            Case2Vertex currentVertex = this.context.vertex().get().getValue();
            // iterate over currentVertex's prevAncestors
            // if prevAncestor's ancestorID == currentVertexID, then ringCount++
            messageIterator.forEachRemaining(message -> message.forEach(prevAncestorPair -> {
                if (prevAncestorPair.getRight().equals(vertexId)) {
                    currentVertex.setRingCount(currentVertex.getRingCount() + 1);
                }
            }));
        }
    }
}
