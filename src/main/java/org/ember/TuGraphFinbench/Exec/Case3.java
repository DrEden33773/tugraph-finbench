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
import org.ember.TuGraphFinbench.Cell.Case3Cell;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case3Vertex;
import org.ember.TuGraphFinbench.Sink.OrderedFileSink;
import org.ember.TuGraphFinbench.Source.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class Case3 {

    public static final Logger LOGGER = LoggerFactory.getLogger(Case3.class);
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
    public static String ABSOLUTE_PREFIX = null;
    public static String RESULT_FILE_PATH = "./target/tmp/data/result/finbench";

    static IPipelineResult<?> submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        final Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCtx -> {
            final PWindowSource<IVertex<Long, Case3Vertex>> vertices = pipelineTaskCtx.buildSource(
                    new DataSource<>(vertexFilePaths, vertexParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Double>> edges = pipelineTaskCtx.buildSource(
                    new DataSource<>(edgeFilePaths, edgeParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("Case3Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(BackendType.Memory)
                    .build();

            final PGraphWindow<Long, Case3Vertex, Double> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<Case3Cell> orderedSink = new OrderedFileSink<>("result3.csv");

            graphWindow.compute(new Case3Algorithm(2))
                    .compute(Env.PARALLELISM_MAX)
                    .getVertices()
                    .filter(vertex -> vertex.getValue().isHasIn() && vertex.getValue().isHasOut())
                    .map(vertex -> vertex.getValue().toCase3Cell())
                    .sink(orderedSink)
                    .withParallelism(Env.SINK_PARALLELISM_MAX);
        });

        return pipeline.execute();
    }

    public static void main(String[] args) {
        if (args.length == 3) {
            ABSOLUTE_PREFIX = args[1];
            RESULT_FILE_PATH = args[2];
        }
        LOGGER.info("*** Start Case3 ***");
        final Environment environment = EnvironmentUtil.loadEnvironment(args);
        final IPipelineResult<?> result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
        LOGGER.info("*** End Case3 ***");
    }
}
