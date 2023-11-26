package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import org.ember.TuGraphFinbench.Record.Case4Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;

import java.util.*;


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
        return new Case4ComputeFunction();
    }

    @Override
    public VertexCentricCombineFunction<Case4Message> getCombineFunction() {
        return null;
    }

    public static class Case4ComputeFunction extends AbstractVcFunc<Long, Case4Vertex, Null, Case4Message> {
        @Override
        public void compute(final Long currVertexGId, final Iterator<Case4Message> messageIterator) {
            switch ((int) this.context.getIterationId()) {
                case 1:
                    initIter();
                    break;
                case 2:
                    on0Layer(messageIterator);
                    break;
                case 3:
                    on1Layer(currVertexGId, messageIterator);
                    break;
                case 4:
                    on2Layer(currVertexGId, messageIterator);
                    break;
                case 5:
                    on3Layer(messageIterator);
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
        }

        void on0Layer(final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            // all Person vertices' highest layer must be 0
            currV.setHighestLayer(0);
            if (messageIterator.hasNext()) {
                // update: currV.ahead0LayerLoanAmountMap
                final Map<Long, Double> currAhead0LayerLoanAmountMap = new HashMap<>();
                while (messageIterator.hasNext()) {
                    currAhead0LayerLoanAmountMap.putAll(messageIterator.next().ahead0LayerLoanAmountMap);
                }
                currV.setAhead0LayerLoanAmountMap(currAhead0LayerLoanAmountMap);
            }
            // send message
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), currV.toCase4Message()));
        }

        void on1Layer(final Long currVertexGId, final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            if (currV.getHighestLayer() != 0) {
                return;
            }
            final Map<Long, Double> currAhead1LayerLoanAmountMap = new HashMap<>();
            final List<Case4Message> filteredOutMessages = new ArrayList<>();
            while (messageIterator.hasNext()) {
                final Case4Message currMessage = messageIterator.next();
                if (currMessage.highestLayer != 0) {
                    filteredOutMessages.add(currMessage);
                    continue;
                }
                // update self.highestLayer
                currV.setHighestLayer(1);
                currAhead1LayerLoanAmountMap.putAll(currMessage.ahead0LayerLoanAmountMap);
            }
//            // self.origin.highLayer is 0
//            // has in edge, self highest layer <- 1
//            currV.setHighestLayer(1);
//            // update: currV.ahead1LayerLoanAmountMap := MERGE(messageIterator.all.ahead0LayerLoanAmountMap)
//            while (messageIterator.hasNext()) {
//                currAhead1LayerLoanAmountMap.putAll(messageIterator.next().ahead0LayerLoanAmountMap);
//            }
            currV.setAhead1LayerLoanAmountMap(currAhead1LayerLoanAmountMap);
            // send message
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), currV.toCase4Message()));
            // send filtered out messages (back to self)
            filteredOutMessages.forEach(message -> this.context.sendMessage(currVertexGId, message));
        }

        void on2Layer(final Long currVertexGId, final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            if (currV.getHighestLayer() != 1) {
                return;
            }
            final Map<Long, Double> currAhead2LayerLoanAmountMap = new HashMap<>();
            final List<Case4Message> filteredOutMessages = new ArrayList<>();
            while (messageIterator.hasNext()) {
                final Case4Message currMessage = messageIterator.next();
                if (currMessage.highestLayer != 1) {
                    filteredOutMessages.add(currMessage);
                    continue;
                }
                // update self.highestLayer
                currV.setHighestLayer(2);
                currAhead2LayerLoanAmountMap.putAll(currMessage.ahead1LayerLoanAmountMap);
            }
//            // self.origin.highLayer is 1
//            // has in edge, self highest layer <- 2
//            currV.setHighestLayer(2);
//            // update: currV.ahead2LayerLoanAmountMap := MERGE(messageIterator.foreach.ahead1LayerLoanAmountMap)
//            while (messageIterator.hasNext()) {
//                currAhead2LayerLoanAmountMap.putAll(messageIterator.next().ahead1LayerLoanAmountMap);
//            }
            currV.setAhead2LayerLoanAmountMap(currAhead2LayerLoanAmountMap);
            // send message
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), currV.toCase4Message()));
            // send filtered out messages (back to self)
            filteredOutMessages.forEach(message -> this.context.sendMessage(currVertexGId, message));
        }

        void on3Layer(final Iterator<Case4Message> messageIterator) {
            final Case4Vertex currV = this.context.vertex().get().getValue();
            if (currV.getVertexType() == VertexType.Loan) {
                return;
            }
            if (!(currV.getHighestLayer() == 2 && messageIterator.hasNext())) {
                return;
            }
            // self.origin.highLayer is 2
            // has in edge, self highest layer <- 3
            currV.setHighestLayer(3);
            // update: currV.ahead3LayerLoanAmountMap := MERGE(messageIterator.foreach.ahead2LayerLoanAmountMap)
            final Map<Long, Double> currAhead3LayerLoanAmountMap = new HashMap<>();
            while (messageIterator.hasNext()) {
                currAhead3LayerLoanAmountMap.putAll(messageIterator.next().ahead2LayerLoanAmountMap);
            }
            currV.setAhead3LayerLoanAmountMap(currAhead3LayerLoanAmountMap);
            // done, leave `transfer unit` to `vertex to cell`
        }
    }
}
