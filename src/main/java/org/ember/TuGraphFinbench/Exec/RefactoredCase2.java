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
import com.antgroup.geaflow.model.common.Null;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import org.ember.TuGraphFinbench.Algorithms.Case2Algorithm;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case2Vertex;
import org.ember.TuGraphFinbench.Source.RefactoredDataSource;

import java.util.ArrayList;
import java.util.Collections;

public class RefactoredCase2 {

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/finbench/case2";
    public static final String[] vertexFilePaths = {"Account.csv"};
    public static final String[] edgeFilePaths = {"AccountTransferAccount.csv"};
    public static final FileLineParser<IVertex<Long, Case2Vertex>>[] vertexParsers = new FileLineParser[]{(String line) -> {
        final String[] fields = line.split("\\|");
        final long accountID = Long.parseLong(fields[0]);
        final Case2Vertex case2Vertex = new Case2Vertex(accountID, 0, new ArrayList<>());
        final IVertex<Long, Case2Vertex> vertex = new ValueVertex<>(accountID, case2Vertex);
        return Collections.singletonList(vertex);
    }};
    public static final FileLineParser<IEdge<Long, Null>>[] edgeParsers = new FileLineParser[]{(String line) -> {
        final String[] fields = line.split("\\|");
        final long srcID = Long.parseLong(fields[0]);
        final long dstID = Long.parseLong(fields[1]);
        final IEdge<Long, Null> edge = new ValueEdge<>(srcID, dstID, new Null());
        return Collections.singletonList(edge);
    }};

    private static IPipelineResult<?> submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        final Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCtx -> {
            final PWindowSource<IVertex<Long, Case2Vertex>> vertices = pipelineTaskCtx.buildSource(
                    new RefactoredDataSource<>(vertexFilePaths, vertexParsers), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Null>> edges = pipelineTaskCtx.buildSource(
                    new RefactoredDataSource<>(edgeFilePaths, edgeParsers), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("Case2Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            final PGraphWindow<Long, Case2Vertex, Null> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<String> sink = new FileSink<>();

            graphWindow.compute(new Case2Algorithm(4))
                    .compute(Env.PARALLELISM_MAX)
                    .getVertices()
                    .filter(vertex -> vertex.getValue().getRingCount() > 0)
                    .map(vertex -> vertex.getValue().getID() + "|" + vertex.getValue().getRingCount())
                    .sink(sink)
                    .withParallelism(1);
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
