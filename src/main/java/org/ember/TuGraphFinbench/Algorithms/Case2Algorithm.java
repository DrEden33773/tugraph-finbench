package org.ember.TuGraphFinbench.Algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.ember.TuGraphFinbench.Record.Case2Vertex;

import java.util.Arrays;
import java.util.Iterator;

public class Case2Algorithm extends VertexCentricCompute<Long, Case2Vertex, Boolean, ImmutableTriple<Long, long[], long[]>> {

    public Case2Algorithm() {
        super(4);
    }

    public Case2Algorithm(long iterations) {
        super(iterations);
        assert iterations == 2;
    }

    @Override
    public VertexCentricCombineFunction<ImmutableTriple<Long, long[], long[]>> getCombineFunction() {
        return null;
    }

    @Override
    public VertexCentricComputeFunction<Long, Case2Vertex, Boolean, ImmutableTriple<Long, long[], long[]>> getComputeFunction() {
        return new Case2ComputeFunction();
    }

    public static class Case2ComputeFunction extends AbstractVcFunc<Long, Case2Vertex, Boolean, ImmutableTriple<Long, long[], long[]>> {
        @Override
        public void compute(final Long vertexId, final Iterator<ImmutableTriple<Long, long[], long[]>> messageIterator) {
            switch ((int) this.context.getIterationId()) {
                case 1:
                    // 每个节点将自己的出边邻接表发送给自己所有的入边邻居
                    computeIter1(vertexId);
                    break;
                case 2:
                    // 每个节点根据接收到的入边邻居的邻接表，利用集合交集计算（入边邻居的出边邻接表 交 自己的入边邻接表），完成三角形计数（需考虑边的重数信息）
                    computeIter2(vertexId, messageIterator);
                    break;
                default:
                    throw new RuntimeException("Invalid iteration id: " + this.context.getIterationId());
            }
        }

        void computeIter1(final Long vertexId) {
            // 生成带有重数的出边邻接表
            Long2LongOpenHashMap rawAdjList = new Long2LongOpenHashMap();
            this.context.edges().getOutEdges().forEach(edge -> {
                if ((edge.getTargetId() != edge.getSrcId()) && (edge.getValue())) {
                    rawAdjList.addTo(edge.getTargetId(), 1);
                    //System.out.println("---> Iter 1: out edge: " + vertexId + " -> " + edge.getTargetId());
                }
            });
            long[] neighborVids = rawAdjList.keySet().toLongArray();
            Arrays.sort(neighborVids);
            long[] numEdges = new long[neighborVids.length];
            for (int i = 0; i < neighborVids.length; i++)
                numEdges[i] = rawAdjList.get(neighborVids[i]);
            // 生成不重复的入边邻接表
            Long2LongOpenHashMap inNeighborSet = new Long2LongOpenHashMap();
            this.context.edges().getOutEdges().forEach(edge -> {
                if (/*edge.getTargetId() != edge.getSrcId() &&*/ !edge.getValue()) {
                    inNeighborSet.addTo(edge.getTargetId(), 1);
                    //System.out.println("---> Iter 1: in edge: " + edge.getTargetId() + " -> " + vertexId);
                }
            });
            // 向所有入边邻居发送出边邻接表
            ImmutableTriple<Long, long[], long[]> msg = new ImmutableTriple<Long, long[], long[]>(vertexId, neighborVids, numEdges);
            for (long inSrcId : inNeighborSet.keySet()) {
                this.context.sendMessage(inSrcId, msg);
            }
            //System.out.println("----> Iter1: vertex " + vertexId + ", in neighbor: " + inNeighborSet.size());
            //this.context.edges().getInEdges().forEach(edge -> this.context.sendMessage(edge.getSrcId(), msg));
        }

        void computeIter2(final Long vertexId, final Iterator<ImmutableTriple<Long, long[], long[]>> messageIterator) {
            Case2Vertex currentVertex = this.context.vertex().get().getValue();
            // 生成当前节点的带有重数的入边邻接表
            Long2LongOpenHashMap localInAdjList = new Long2LongOpenHashMap();
            this.context.edges().getOutEdges().forEach(edge -> {
                if (edge.getTargetId() != edge.getSrcId() && !edge.getValue()) // 忽略自环
                    localInAdjList.addTo(edge.getTargetId(), 1);
            });
            //System.out.println("---> Iter 2 vetex " + vertexId + ", local in adj len " + localInAdjList.size());
            // 生成当前节点的带有重数的出边邻接表
            Long2LongOpenHashMap localOutAdjList = new Long2LongOpenHashMap();
            this.context.edges().getOutEdges().forEach(edge -> {
                if (/*edge.getTargetId() != edge.getSrcId() &&*/ edge.getValue()) { // 忽略自环
                    localOutAdjList.addTo(edge.getTargetId(), 1);
                }
            });
            //System.out.println("---> Iter 2 vetex " + vertexId + ", local out adj len " + localOutAdjList.size());
            long totalCount = 0;
            // 接收来自出边邻居的出边邻接表
            while (messageIterator.hasNext()) {
                final ImmutableTriple<Long, long[], long[]> neighborMsg = messageIterator.next();
                long neighborVid = neighborMsg.getLeft();
                // 构造对应的OpenHashMap
                Long2LongOpenHashMap neighborOutAdjList = new Long2LongOpenHashMap();
                for (int i = 0; i < neighborMsg.getMiddle().length; i++) {
                    long vid = neighborMsg.getMiddle()[i];
                    long numEdge = neighborMsg.getRight()[i];
                    neighborOutAdjList.put(vid, numEdge);
                }
                //System.out.println("---> Iter 2 vetex " + vertexId + ", neighborVid " + neighborVid + ", out adj len " + neighborOutAdjList.size());
                for (long candidateVid : neighborOutAdjList.keySet()) {
                    //System.out.println("---> Iter 2 vetex " + vertexId + ", candidateVid " + candidateVid + ", local in adj[0] " + localInAdjList.);
                    if (localInAdjList.containsKey(candidateVid)) {
                        // 发现三角形回路 vertexId -[e1]-> neighborVid -[e2]-> candidateVid, vertexId <-[e3]- candidateVid
                        long ne1 = localOutAdjList.get(neighborVid);
                        long ne2 = neighborOutAdjList.get(candidateVid);
                        long ne3 = localInAdjList.get(candidateVid);
                        assert ne1 >= 1;
                        assert ne2 >= 1;
                        assert ne3 >= 1;
                        long count = ne1 * ne2 * ne3;
                        totalCount += count;
                    }
                }
            }
            //System.out.println("---> Iter 2 vetex " + vertexId + ", totalCount " + totalCount);
            currentVertex.setRingCount(totalCount);
        }

    }
}
