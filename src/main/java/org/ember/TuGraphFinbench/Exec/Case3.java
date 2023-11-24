package org.ember.TuGraphFinbench.Exec;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource.FileLineParser;
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
import org.ember.TuGraphFinbench.Algorithms.Case3Algorithm;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case3Vertex;
import org.ember.TuGraphFinbench.Source.DataSource;

import java.util.Collections;

public class Case3 {

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/finbench/case3";
    public static final String[] vertexFilePaths = {"Account.csv"};
    public static final String[] edgeFilePaths = {"AccountTransferAccount.csv"};
    public static final FileLineParser<IVertex<Long, Case3Vertex>>[] vertexParsers = new FileLineParser[]{(final String line) -> {
        final String[] fields = line.split("\\|");
        final long accountID = Long.parseLong(fields[0]);
        final Case3Vertex case3Vertex = new Case3Vertex(accountID, 0, 0, 0, false, false);
        final IVertex<Long, Case3Vertex> vertex = new ValueVertex<>(accountID, case3Vertex);
        return Collections.singletonList(vertex);
    }};
    public static final FileLineParser<IEdge<Long, Double>>[] edgeParsers = new FileLineParser[]{(final String line) -> {
        final String[] fields = line.split("\\|");
        final long srcID = Long.parseLong(fields[0]);
        final long dstID = Long.parseLong(fields[1]);
        final double transferAmount = Double.parseDouble(fields[2]);
        final IEdge<Long, Double> edge = new ValueEdge<>(srcID, dstID, transferAmount);
        return Collections.singletonList(edge);
    }};

    static IPipelineResult<?> submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        final Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCtx -> {
            final PWindowSource<IVertex<Long, Case3Vertex>> vertices = pipelineTaskCtx.buildSource(
                    new DataSource<>(vertexFilePaths, vertexParsers), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Double>> edges = pipelineTaskCtx.buildSource(
                    new DataSource<>(edgeFilePaths, edgeParsers), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("Case3Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(BackendType.Memory)
                    .build();

            final PGraphWindow<Long, Case3Vertex, Double> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<String> sink = new FileSink<>();

            graphWindow.compute(new Case3Algorithm(2))
                    .compute(Env.PARALLELISM_MAX)
                    .getVertices()
                    .filter(vertex -> vertex.getValue().isHasIn() && vertex.getValue().isHasOut())
                    .map(vertex -> vertex.getValue().getID() + "|" + vertex.getValue().getInOutRatio())
                    .sink(sink)
                    .withParallelism(Env.SINK_PARALLELISM_MAX);
        });

        return pipeline.execute();
    }

    public static void main(String[] args) {
        final Environment environment = EnvironmentUtil.loadEnvironment(args);
        final IPipelineResult<?> result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }
}
