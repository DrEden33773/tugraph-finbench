package org.ember.TuGraphFinbench.Exec;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
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
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case4Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;
import org.ember.TuGraphFinbench.Source.DataSource;

import java.util.Collections;

public class Case4 {

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/finbench/case4";
    public static final String[] vertexFilePaths = {"Person.csv", "Loan.csv"};
    public static final String[] edgeFilePaths = {"PersonGuaranteePerson.csv", "PersonApplyLoan.csv"};
    public static final FileSource.FileLineParser<IVertex<Long, Case4Vertex>>[] vertexParsers = new FileSource.FileLineParser[]{
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long personID = Long.parseLong(fields[0]);
                final Case4Vertex case4Vertex = new Case4Vertex(VertexType.Person, personID, 0);
                final IVertex<Long, Case4Vertex> vertex = new ValueVertex<>(personID, case4Vertex);
                return Collections.singletonList(vertex);
            }, // Person.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long loanID = Long.parseLong(fields[0]);
                final double loanAmount = Double.parseDouble(fields[1]);
                final Case4Vertex case4Vertex = new Case4Vertex(VertexType.Loan, loanID, loanAmount);
                final IVertex<Long, Case4Vertex> vertex = new ValueVertex<>(loanID, case4Vertex);
                return Collections.singletonList(vertex);
            }, // Loan.csv
    };
    public static final FileSource.FileLineParser<IEdge<Long, Null>>[] edgeParsers = new FileSource.FileLineParser[]{
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long fromPersonID = Long.parseLong(fields[0]);
                final long toPersonID = Long.parseLong(fields[1]);
                // invert the edge direction while loading
                final IEdge<Long, Null> edge = new ValueEdge<>(toPersonID, fromPersonID, new Null());
                return Collections.singletonList(edge);
            }, // PersonGuaranteePerson.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long fromPersonID = Long.parseLong(fields[0]);
                final long toLoanID = Long.parseLong(fields[1]);
                // invert the edge direction while loading
                final IEdge<Long, Null> edge = new ValueEdge<>(toLoanID, fromPersonID, new Null());
                return Collections.singletonList(edge);
            }, // PersonApplyLoan.csv
    };

    private static IPipelineResult<?> submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        final Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCtx -> {
            final PWindowSource<IVertex<Long, Case4Vertex>> vertices = pipelineTaskCtx.buildSource(
                    new DataSource<>(vertexFilePaths, vertexParsers), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Null>> edges = pipelineTaskCtx.buildSource(
                    new DataSource<>(edgeFilePaths, edgeParsers), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("Case2Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            final PGraphWindow<Long, Case4Vertex, Null> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<String> sink = new FileSink<>();

            // TODO -> Case4Algorithm
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
