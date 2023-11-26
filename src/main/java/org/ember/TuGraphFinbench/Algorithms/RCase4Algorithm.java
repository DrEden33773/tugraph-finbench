package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.ember.TuGraphFinbench.Record.RCase4Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;

import java.util.Iterator;

public class RCase4Algorithm extends VertexCentricCompute<Long, RCase4Vertex, Null, ImmutablePair<Long, Double>> {
    public RCase4Algorithm() {
        super(5);
    }

    public RCase4Algorithm(long iterations) {
        super(iterations);
        assert iterations == 5;
    }

    @Override
    public VertexCentricComputeFunction<Long, RCase4Vertex, Null, ImmutablePair<Long, Double>> getComputeFunction() {
        return new RCase4ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<ImmutablePair<Long, Double>> getCombineFunction() {
        return null;
    }

    public static class RCase4ComputeFunction extends AbstractVcFunc<Long, RCase4Vertex, Null, ImmutablePair<Long, Double>> {
        @Override
        public void compute(final Long currVertexGId, final Iterator<ImmutablePair<Long, Double>> messageIterator) {
            switch ((int) this.context.getIterationId()) {
                case 1:
                    sendLoans();
                    break;
                case 2:
                    calcLoanSum(messageIterator);
                    break;
                case 3:
                case 4:
                    gatherMessagesFromOthersThenSend(messageIterator);
                    break;
                case 5:
                    gatherMessagesFromOthersOnly(messageIterator);
                    break;
                default:
                    throw new RuntimeException("Invalid iteration id: " + this.context.getIterationId());
            }
        }

        void sendLoans() {
            final RCase4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() != VertexType.Loan) {
                return;
            }
            final long ignore = 0;
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(ignore, currV.getSelfLoanAmountSum())));
        }

        void calcLoanSum(final Iterator<ImmutablePair<Long, Double>> messages) {
            final RCase4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            double currLoanSum = 0.0;
            while (messages.hasNext()) {
                final ImmutablePair<Long, Double> msg = messages.next();
                currLoanSum += msg.getRight();
            }
            currV.setSelfLoanAmountSum(currLoanSum);
            // send to others
            double finalCurrLoanSum = currLoanSum;
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(currV.getID(), finalCurrLoanSum)));
        }

        void gatherMessagesFromOthersThenSend(final Iterator<ImmutablePair<Long, Double>> messages) {
            final RCase4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            while (messages.hasNext()) {
                final ImmutablePair<Long, Double> msg = messages.next();
                currV.getReceivedPersonLoanAmountMap().put(msg.getLeft(), msg.getRight());
            }
            // gathered, send message to others
            this.context.edges().getOutEdges().forEach(edge -> {
                        currV.getReceivedPersonLoanAmountMap().forEach((k, v) -> this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(k, v)));
                    }
            );
        }

        void gatherMessagesFromOthersOnly(final Iterator<ImmutablePair<Long, Double>> messages) {
            final RCase4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            while (messages.hasNext()) {
                final ImmutablePair<Long, Double> msg = messages.next();
                currV.getReceivedPersonLoanAmountMap().put(msg.getLeft(), msg.getRight());
            }
            // done, no need to send anything out
        }
    }
}
