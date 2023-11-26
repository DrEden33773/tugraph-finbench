package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.common.Null;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.ember.TuGraphFinbench.Record.Case1Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

public class Case1Algorithm extends VertexCentricCompute<Long, Case1Vertex, Null, ImmutablePair<Long, Double>> {

    public Case1Algorithm() {
        super(4);
    }

    public Case1Algorithm(long iterations) {
        super(iterations);
        assert iterations == 4;
    }

    @Override
    public VertexCentricCombineFunction<ImmutablePair<Long, Double>> getCombineFunction() {
        return null;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case1Vertex, Null, ImmutablePair<Long, Double>> getComputeFunction() {
        return new Case1ComputeFunction();
    }

    public static class Case1ComputeFunction extends AbstractVcFunc<Long, Case1Vertex, Null, ImmutablePair<Long, Double>> {

        @Override
        public void compute(final Long vertexId, final Iterator<ImmutablePair<Long, Double>> messageIterator) {
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
            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(currVertex.getID(), currVertex.getLoanAmountSum())));
//            this.context.sendMessageToNeighbors(new ImmutablePair<>(1, currVertex.getLoanAmountSum()));
        }

        public void computeIter2(final Iterator<ImmutablePair<Long, Double>> messageIterator) {
            final Case1Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Account) {
                return;
            }
            // update: layer0LoanAmountSum, nthLayer
            double loanAmountSum = currVertex.getLoanAmountSum();
            Map<Long, Double> loanAmountMap = new HashMap<Long, Double>();
            while (messageIterator.hasNext()) {
                final ImmutablePair<Long, Double> message = messageIterator.next();
                loanAmountMap.put(message.getLeft(), message.getRight());
            }
            //System.out.println("-----> Iter2: vertex " + currVertex.getID() + " get " + loanAmountMap.size() + " loans.");
            this.context.edges().getOutEdges().forEach(edge -> {
              if (((edge.getTargetId() & 3) == 2) && (edge.getTargetId() != edge.getSrcId())) {
                // the target is an account vertex
                // send the received loans to its outgoing account neighbors
                for(Map.Entry<Long, Double> entry: loanAmountMap.entrySet()) {
                  this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(entry.getKey(), entry.getValue()));
                }
              }
            });
            //this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(2, finalLoanAmountSum)));
//            this.context.sendMessageToNeighbors(new ImmutablePair<>(2, loanAmountSum));
        }

        public void computeIter3(final Iterator<ImmutablePair<Long, Double>> messageIterator) {
            final Case1Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Account) {
                return;
            }
            // update: layer0LoanAmountSum, nthLayer
            Map<Long, Double> loanAmountMap = new HashMap<Long, Double>();
            double loanAmountSum = currVertex.getLoanAmountSum();
            while (messageIterator.hasNext()) {
                final ImmutablePair<Long, Double> message = messageIterator.next();
                loanAmountMap.put(message.getLeft(), message.getRight());
            }
            //System.out.println("-----> Iter3: vertex " + currVertex.getID() + " get " + loanAmountMap.size() + " loans.");
            this.context.edges().getOutEdges().forEach(edge -> {
              if ((edge.getTargetId() & 3)  == 1) {
                // the target is a Person vertex
                // send the received loans to its outgoing Person neighbors
                for(Map.Entry<Long, Double> entry: loanAmountMap.entrySet()) {
                  this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(entry.getKey(), entry.getValue()));
                }
              }
            });

//            this.context.edges().getOutEdges().forEach(edge -> this.context.sendMessage(edge.getTargetId(), new ImmutablePair<>(3, finalLoanAmountSum)));
//            this.context.sendMessageToNeighbors(new ImmutablePair<>(3, loanAmountSum));
        }

        public void computeIter4(final Iterator<ImmutablePair<Long, Double>> messageIterator) {
            final Case1Vertex currVertex = this.context.vertex().get().getValue();
            if (currVertex.getVertexType() != VertexType.Person) {
                return;
            }
            // update: layer0LoanAmountSum
            Map<Long, Double> loanAmountMap = new HashMap<Long, Double>();
            while (messageIterator.hasNext()) {
                final ImmutablePair<Long, Double> message = messageIterator.next();
                loanAmountMap.put(message.getLeft(), message.getRight());
            }

            if (loanAmountMap.size() != 0) {
              currVertex.setNthLayer(4);
              double loanAmountSum = 0.0;
              for(Map.Entry<Long, Double> entry: loanAmountMap.entrySet()) {
                loanAmountSum += entry.getValue();
              }
              loanAmountSum /= 1e8;
              currVertex.setLoanAmountSum(loanAmountSum);
            }
//            currVertex.setLoanAmountSum(loanAmountSum);
            // unit transfer
            // #.00 -> deprecated, let `String.format("%.2f", loanAmountSum)` do this instead
            // final DecimalFormat dFormat = new DecimalFormat("#.00");
            // loanAmountSum = Double.parseDouble(dFormat.format(loanAmountSum));
            // update: layer0LoanAmountSum
        }
    }
}
