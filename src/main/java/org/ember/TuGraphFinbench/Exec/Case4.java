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
import org.ember.TuGraphFinbench.Algorithms.Case4Algorithm;
import org.ember.TuGraphFinbench.Cell.Case4Cell;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case4Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;
import org.ember.TuGraphFinbench.Sink.OrderedFileSink;
import org.ember.TuGraphFinbench.Source.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class Case4 {

    public static final Logger LOGGER = LoggerFactory.getLogger(Case4.class);
    public static final String[] vertexFilePaths = {"Person.csv", "Loan.csv"};
    public static final String[] edgeFilePaths = {"PersonGuaranteePerson.csv", "PersonApplyLoan.csv"};
    public static final FileSource.FileLineParser<IVertex<Long, Case4Vertex>>[] vertexParsers = new FileSource.FileLineParser[]{
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long personID = Long.parseLong(fields[0]);
                final Case4Vertex case4Vertex = new Case4Vertex(VertexType.Person, personID, 0, 0, 0, 0, 0);
                final IVertex<Long, Case4Vertex> vertex = new ValueVertex<>(personID, case4Vertex);
                return Collections.singletonList(vertex);
            }, // Person.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long loanID = Long.parseLong(fields[0]);
                final double loanAmount = Double.parseDouble(fields[1]);
                final Case4Vertex case4Vertex = new Case4Vertex(VertexType.Loan, loanID, loanAmount, 0, 0, 0, 0);
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
    public static String ABSOLUTE_PREFIX = null;
    public static String RESULT_FILE_PATH = "./target/tmp/data/result/finbench";

    private static IPipelineResult<?> submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        final Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCtx -> {
            final PWindowSource<IVertex<Long, Case4Vertex>> vertices = pipelineTaskCtx.buildSource(
                    new DataSource<>(vertexFilePaths, vertexParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Null>> edges = pipelineTaskCtx.buildSource(
                    new DataSource<>(edgeFilePaths, edgeParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("Case2Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            final PGraphWindow<Long, Case4Vertex, Null> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<Case4Cell> orderedSink = new OrderedFileSink<>("result4.csv");

            graphWindow.compute(new Case4Algorithm(5))
                    .compute(Env.PARALLELISM_MAX)
                    .getVertices()
                    .filter(vertex -> vertex.getValue().getHighestLayer() > 0)
                    .map(vertex -> vertex.getValue().toCase4Cell())
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
        LOGGER.info("*** Start Case4 ***");
        final Environment environment = EnvironmentUtil.loadEnvironment(args);
        final IPipelineResult<?> result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
        LOGGER.info("*** End Case4 ***");
    }
}
