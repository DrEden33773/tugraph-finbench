package org.ember.TuGraphFinbench;

import java.util.Iterator;

import org.ember.TuGraphFinbench.Builder.EdgeBuilder;
import org.ember.TuGraphFinbench.Builder.VertexBuilder;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Edge;
import org.ember.TuGraphFinbench.Record.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;

public class LoadDataOnlyDemo {

    static final Logger LOGGER = LoggerFactory.getLogger(LoadDataOnlyDemo.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/finbench";

    static IPipelineResult<?> submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCtx) {
                Configuration conf = pipelineTaskCtx.getConfig();

                PWindowSource<IVertex<Long, Node>> vertices = pipelineTaskCtx
                        .buildSource(new VertexBuilder(), AllWindow.getInstance())
                        .withParallelism(Env.PARALLELISM_MAX);

                PWindowSource<IEdge<Long, Edge>> edges = pipelineTaskCtx
                        .buildSource(new EdgeBuilder(), AllWindow.getInstance())
                        .withParallelism(Env.PARALLELISM_MAX);

                GraphViewDesc graphViewDesc = GraphViewBuilder
                        .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                        .withShardNum(Env.PARALLELISM_MAX)
                        .withBackend(BackendType.Memory)
                        .build();

                PGraphWindow<Long, Node, Edge> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices, edges,
                        graphViewDesc);

                SinkFunction<IVertex<Long, Node>> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);

                graphWindow.compute(new Algorithm(1))
                        .compute(Env.PARALLELISM_MAX)
                        .getVertices()
                        .map((e) -> {
                            // TODO
                            return e;
                        })
                        .sink(sink)
                        .withParallelism(Env.PARALLELISM_MAX);
            }
        });

        return pipeline.execute();
    }

    public static class Algorithm extends VertexCentricCompute<Long, Node, Edge, Node> {

        public Algorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Long, Node, Edge, Node> getComputeFunction() {
            return new ComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Node> getCombineFunction() {
            return null;
        }

        public class ComputeFunction extends AbstractVcFunc<Long, Node, Edge, Node> {

            @Override
            public void compute(Long vertexId, Iterator<Node> messageIterator) {
                LOGGER.info(">> DEMO-MODE (No computation, only display) <<");
                LOGGER.info("Passed: {} (VertexID)", vertexId);
            }

        }
    }

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult<?> result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
        LOGGER.info("!!!!! Have loaded data successfully !!!!!");
    }
}
