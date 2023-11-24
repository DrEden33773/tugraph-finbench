package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.ember.TuGraphFinbench.Record.Case1Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;

import java.text.DecimalFormat;
import java.util.Iterator;

public class Case1Algorithm extends VertexCentricCompute<Long, Case1Vertex, Null, ImmutablePair<Integer, Double>> {

    public Case1Algorithm(long iterations) {
        super(iterations);
        assert iterations == 4;
    }

    @Override
    public VertexCentricCombineFunction<ImmutablePair<Integer, Double>> getCombineFunction() {
        return null;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case1Vertex, Null, ImmutablePair<Integer, Double>> getComputeFunction() {
        return new Case1ComputeFunction();
    }

    public static class Case1ComputeFunction extends AbstractVcFunc<Long, Case1Vertex, Null, ImmutablePair<Integer, Double>> {

        @Override
        public void compute(final Long vertexId, final Iterator<ImmutablePair<Integer, Double>> messageIterator) {
            switch ((int) this.context.getIterationId()) {
                case 1:
                    computeIter1();
                    break;
                case 2:
                    computeIter2(messageIterator);
                    break;
                case 3:
                    computeIter3(messageIterator);
                    break;
                case 4:
                    computeIter4(messageIterator);
                    break;
                default:
                    throw new RuntimeException("Invalid iteration id: " + this.context.getIterationId());
            }
        }

        public void computeIter1() {
            final Case1Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Loan) {
                return;
            }
            currVertex.setNthLayer(1);
            this.context.sendMessageToNeighbors(new ImmutablePair<>(1, currVertex.getLoanAmountSum()));
        }

        public void computeIter2(final Iterator<ImmutablePair<Integer, Double>> messageIterator) {
            final Case1Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Account) {
                return;
            }
            // update: loanAmountSum, nthLayer
            double loanAmountSum = currVertex.getLoanAmountSum();
            if (messageIterator.hasNext() && messageIterator.next().getLeft() == 1) {
                currVertex.setNthLayer(2);
            } else {
                return; // does not satisfy: loan -> account
            }
            while (messageIterator.hasNext()) {
                final ImmutablePair<Integer, Double> message = messageIterator.next();
                if (message.getLeft() == 1) {
                    loanAmountSum += message.getRight();
                }
            }
            currVertex.setLoanAmountSum(loanAmountSum);
            // send: loanAmountSum(updated)
            this.context.sendMessageToNeighbors(new ImmutablePair<>(2, loanAmountSum));
        }

        public void computeIter3(final Iterator<ImmutablePair<Integer, Double>> messageIterator) {
            final Case1Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Account) {
                return;
            }
            // update: loanAmountSum, nthLayer
            double loanAmountSum = currVertex.getLoanAmountSum();
            if (messageIterator.hasNext() && messageIterator.next().getLeft() == 2) {
                currVertex.setNthLayer(3);
            } else {
                return; // does not satisfy: (loan -> account) -> account
            }
            while (messageIterator.hasNext()) {
                final ImmutablePair<Integer, Double> message = messageIterator.next();
                if (message.getLeft() == 2) {
                    loanAmountSum += message.getRight();
                }
            }
            currVertex.setLoanAmountSum(loanAmountSum);
            // send: loanAmountSum(updated)
            this.context.sendMessageToNeighbors(new ImmutablePair<>(3, loanAmountSum));
        }

        public void computeIter4(final Iterator<ImmutablePair<Integer, Double>> messageIterator) {
            final Case1Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Person) {
                return;
            }
            // update: loanAmountSum
            double loanAmountSum = currVertex.getLoanAmountSum();
            if (messageIterator.hasNext() && messageIterator.next().getLeft() == 3) {
                currVertex.setNthLayer(4);
            } else {
                return; // does not satisfy: ((loan -> account) -> account) -> person
            }
            while (messageIterator.hasNext()) {
                final ImmutablePair<Integer, Double> message = messageIterator.next();
                if (message.getLeft() == 3) {
                    loanAmountSum += message.getRight();
                }
            }
            currVertex.setLoanAmountSum(loanAmountSum);
            // unit transfer
            loanAmountSum /= 1e8;
            // #.00
            final DecimalFormat dFormat = new DecimalFormat("#.00");
            loanAmountSum = Double.parseDouble(dFormat.format(loanAmountSum));
            // update: loanAmountSum
            currVertex.setLoanAmountSum(loanAmountSum);
        }
    }
}
