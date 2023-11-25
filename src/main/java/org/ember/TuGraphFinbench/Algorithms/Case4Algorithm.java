package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import org.ember.TuGraphFinbench.Record.Case4Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;

import java.util.Iterator;


public class Case4Algorithm extends VertexCentricCompute<Long, Case4Vertex, Null, Case4Message> {

    public Case4Algorithm() {
        super(5);
    }

    public Case4Algorithm(long iterations) {
        super(iterations);
        assert iterations == 5;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case4Vertex, Null, Case4Message> getComputeFunction() {
        return new Case1ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Case4Message> getCombineFunction() {
        return null;
    }

    public static class Case1ComputeFunction extends AbstractVcFunc<Long, Case4Vertex, Null, Case4Message> {
        @Override
        public void compute(final Long vertexId, final Iterator<Case4Message> messageIterator) {
            switch ((int) this.context.getIterationId()) {
                case 1:
                    initIter();
                    break;
                case 2:
                    from0Layer(messageIterator);
                    break;
                case 3:
                    from1Layer(messageIterator);
                    break;
                case 4:
                    from2Layer(messageIterator);
                    break;
                case 5:
                    from3Layer(messageIterator);
                    break;
                default:
                    throw new RuntimeException("Invalid iteration id: " + this.context.getIterationId());
            }
        }

        void initIter() {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() != VertexType.Loan) {
                return;
            }
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), currV.toCase4Message()));
//            this.context.sendMessageToNeighbors(currV.toCase4Message());
        }

        void from0Layer(final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            // update: self loan amount sum (layer0)
            double currVLoanAmountSum = currV.getLayer0LoanAmountSum();
            currV.setHighestLayer(0);
            while (messageIterator.hasNext()) {
                currVLoanAmountSum += messageIterator.next().layer0LoanAmountSum;
            }
            currV.setLayer0LoanAmountSum(currVLoanAmountSum);
            // send message
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), currV.toCase4Message()));
//            this.context.sendMessageToNeighbors(currV.toCase4Message());
        }

        void from1Layer(final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            if (messageIterator.hasNext()) {
                // has in edge, self highest layer <- 1
                currV.setHighestLayer(1);
                // update: self loan amount sum
                // (layer1 <- layer0)
                double currVLayer1LoanAmountSum = currV.getLayer1LoanAmountSum();
                while (messageIterator.hasNext()) {
                    currVLayer1LoanAmountSum += messageIterator.next().layer0LoanAmountSum;
                }
                currV.setLayer1LoanAmountSum(currVLayer1LoanAmountSum);
            }
            // send message
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), currV.toCase4Message()));
//            this.context.sendMessageToNeighbors(currV.toCase4Message());
        }

        void from2Layer(final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            // update: self loan amount sum
            // (layer2 <- (layer1 <- layer0))
            // (layer1 <- layer0)
            while (messageIterator.hasNext()) {
                final Case4Message message = messageIterator.next();
                switch (message.highestLayer) {
                    case 0:
                        currV.setHighestLayer(1);
                        currV.setLayer1LoanAmountSum(currV.getLayer1LoanAmountSum()
                                + message.layer0LoanAmountSum);
                    case 1:
                        currV.setHighestLayer(2);
                        currV.setLayer2LoanAmountSum(currV.getLayer2LoanAmountSum()
                                        + currV.getLayer1LoanAmountSum() // currV.layer1.sum
                                        + message.layer1LoanAmountSum // msg.layer1.sum
                                // currV.layer1 + msg.layer1 -> 1 + 1 = 2 -> currV.highest.layer
                        );
                        break;
                    default:
                        break;
                }
            }
            // send message
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), currV.toCase4Message()));
//            this.context.sendMessageToNeighbors(currV.toCase4Message());
        }

        void from3Layer(final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            // update: self loan amount sum
            // (layer3 <- (layer2 <- (layer1 <- layer0)))
            // (layer2 <- (layer1 <- layer0))
            // (layer1 <- layer0)
            while (messageIterator.hasNext()) {
                final Case4Message message = messageIterator.next();
                switch (message.highestLayer) {
                    case 0:
                        currV.setHighestLayer(1);
                        currV.setLayer1LoanAmountSum(currV.getLayer1LoanAmountSum()
                                + message.layer0LoanAmountSum);
                    case 1:
                        currV.setHighestLayer(2);
                        currV.setLayer2LoanAmountSum(currV.getLayer2LoanAmountSum()
                                        + currV.getLayer1LoanAmountSum() // currV.layer1.sum
                                        + message.layer1LoanAmountSum // msg.layer1.sum
                                // currV.layer1 + msg.layer1 -> 1 + 1 = 2 -> currV.highest.layer
                        );
                    case 2:
                        currV.setHighestLayer(3);
                        currV.setLayer3LoanAmountSum(currV.getLayer3LoanAmountSum()
                                        + currV.getLayer1LoanAmountSum() // currV.layer1.sum
                                        + message.layer2LoanAmountSum // msg.layer2.sum
                                // currV.layer1 + msg.layer2 -> 1 + 2 = 3 -> currV.highest.layer
                        );
                        break;
                    default:
                        break;
                }
            }
            // transfer the unit
            double currVLayer3LoanAmountSum = currV.getLayer3LoanAmountSum();
            double currVLayer2LoanAmountSum = currV.getLayer2LoanAmountSum();
            double currVLayer1LoanAmountSum = currV.getLayer1LoanAmountSum();
            currVLayer1LoanAmountSum /= 1e8;
            currVLayer2LoanAmountSum /= 1e8;
            currVLayer3LoanAmountSum /= 1e8;
            // round half up -> deprecated, use `String.format("%.2f", res)` instead
            // final DecimalFormat dFormat = new DecimalFormat("#.00");
            // currVLayer1LoanAmountSum = Double.parseDouble(dFormat.format(currVLayer1LoanAmountSum)));
            // currVLayer2LoanAmountSum = Double.parseDouble(dFormat.format(currVLayer2LoanAmountSum)));
            // currVLayer3LoanAmountSum = Double.parseDouble(dFormat.format(currVLayer3LoanAmountSum)));
            currV.setLayer1LoanAmountSum(currVLayer1LoanAmountSum);
            currV.setLayer2LoanAmountSum(currVLayer2LoanAmountSum);
            currV.setLayer3LoanAmountSum(currVLayer3LoanAmountSum);
        }
    }
}
