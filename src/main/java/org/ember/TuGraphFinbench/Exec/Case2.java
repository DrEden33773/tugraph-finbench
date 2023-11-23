package org.ember.TuGraphFinbench.Exec;

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
import org.ember.TuGraphFinbench.DataLoader;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case2Vertex;
import org.ember.TuGraphFinbench.Record.RawEdge;
import org.ember.TuGraphFinbench.Record.RawVertex;
import org.ember.TuGraphFinbench.Source.DataSource;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Case2 {
    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/finbench/case2";

    static IPipelineResult<?> submit(Environment environment) {
        final List<IEdge<Long, Null>> edgesRecord = DataLoader.loadEdges().stream()
                .map(RawEdge::toCase2Edge)
                .filter(Objects::nonNull)
                .map(edge -> new ValueEdge<>(edge.getSrcID(), edge.getDstID(), new Null())).collect(Collectors.toList());

        final List<IVertex<Long, Case2Vertex>> verticesRecord = DataLoader.loadVertices().stream()
                .map(RawVertex::toCase2Vertex)
                .filter(Objects::nonNull)
                .map(vertex -> new ValueVertex<>(vertex.getID(), vertex)).collect(Collectors.toList());

        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        final Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCtx -> {
            final PWindowSource<IVertex<Long, Case2Vertex>> vertices = pipelineTaskCtx
                    .buildSource(new DataSource<>(verticesRecord), AllWindow.getInstance())
                    .withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Null>> edges = pipelineTaskCtx
                    .buildSource(new DataSource<>(edgesRecord), AllWindow.getInstance())
                    .withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("Case2Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            final PGraphWindow<Long, Case2Vertex, Null> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<IVertex<Long, Case2Vertex>> sink = new FileSink<>();

            graphWindow.compute(new Case2Algorithm(4))
                    .compute(Env.PARALLELISM_MAX)
                    .getVertices()
                    .filter(vertex -> vertex.getValue().getRingCount() > 0)
                    .sink(sink);
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
