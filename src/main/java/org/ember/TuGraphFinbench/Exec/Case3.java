package org.ember.TuGraphFinbench.Exec;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.ember.TuGraphFinbench.DataLoader;
import org.ember.TuGraphFinbench.Algorithms.Case3Algorithm;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case3Vertex;
import org.ember.TuGraphFinbench.Record.RawEdge;
import org.ember.TuGraphFinbench.Record.RawVertex;
import org.ember.TuGraphFinbench.Source.DataSource;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;

public class Case3 {

        public static final String RESULT_FILE_PATH = "./target/tmp/data/result/finbench/case3";

        static List<IVertex<Long, Case3Vertex>> verticesRecord = DataLoader.rawVertices.stream()
                        .map(RawVertex::toCase3Vertex)
                        .filter(Objects::nonNull)
                        .map(vertex -> new ValueVertex<>(vertex.getID(), vertex)).collect(Collectors.toList());

        static List<IEdge<Long, Double>> edgesRecord = DataLoader.rawEdges.stream()
                        .map(RawEdge::toCase3Edge)
                        .filter(Objects::nonNull)
                        .map(edge -> new ValueEdge<>(edge.getSrcID(), edge.getDstID(), edge.getTransferAmount()))
                        .collect(Collectors.toList());

        static IPipelineResult<?> submit(Environment environment) {
                Pipeline pipeline = PipelineFactory.buildPipeline(environment);
                Configuration envConfig = environment.getEnvironmentContext().getConfig();
                envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
                ResultValidator.cleanResult(RESULT_FILE_PATH);

                pipeline.submit((PipelineTask) pipelineTaskCtx -> {
                        PWindowSource<IVertex<Long, Case3Vertex>> vertices = pipelineTaskCtx
                                        .buildSource(new DataSource<>(verticesRecord), AllWindow.getInstance())
                                        .withParallelism(Env.PARALLELISM_MAX);

                        PWindowSource<IEdge<Long, Double>> edges = pipelineTaskCtx
                                        .buildSource(new DataSource<>(edgesRecord), AllWindow.getInstance())
                                        .withParallelism(Env.PARALLELISM_MAX);

                        GraphViewDesc graphViewDesc = GraphViewBuilder
                                        .createGraphView("Case3Graph")
                                        .withShardNum(Env.PARALLELISM_MAX)
                                        .withBackend(BackendType.Memory)
                                        .build();

                        PGraphWindow<Long, Case3Vertex, Double> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(
                                        vertices,
                                        edges, graphViewDesc);

                        SinkFunction<IVertex<Long, Case3Vertex>> sink = new FileSink<>();

                        graphWindow.compute(new Case3Algorithm(2))
                                        .compute(Env.PARALLELISM_MAX)
                                        .getVertices()
                                        .filter(vertex -> vertex.getValue().isHasIn() && vertex.getValue().isHasOut())
                                        .sink(sink);
                });

                return pipeline.execute();
        }

        public static void main(String[] args) {
                Environment environment = EnvironmentUtil.loadEnvironment(args);
                IPipelineResult<?> result = submit(environment);
                PipelineResultCollect.get(result);
                environment.shutdown();
        }
}
