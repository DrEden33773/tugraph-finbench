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
import org.ember.TuGraphFinbench.Algorithms.RCase4Algorithm;
import org.ember.TuGraphFinbench.Cell.Case4Cell;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.RCase4Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;
import org.ember.TuGraphFinbench.Sink.OrderedFileSink;
import org.ember.TuGraphFinbench.Source.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;

import static org.ember.TuGraphFinbench.Util.globalID;

public class RCase4 {

    public static final Logger LOGGER = LoggerFactory.getLogger(RCase4.class);
    public static final String[] vertexFilePaths = {"Person.csv", "Loan.csv"};
    public static final String[] edgeFilePaths = {"PersonGuaranteePerson.csv", "PersonApplyLoan.csv"};
    public static final FileSource.FileLineParser<IVertex<Long, RCase4Vertex>>[] vertexParsers = new FileSource.FileLineParser[]{
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long personID = Long.parseLong(fields[0]);
                final RCase4Vertex case4Vertex = new RCase4Vertex(VertexType.Person, personID, 0, new HashMap<>());
                // use `gID ~ globalID` as the index
                final long gID = globalID(VertexType.Person, personID);
                final IVertex<Long, RCase4Vertex> vertex = new ValueVertex<>(gID, case4Vertex);
                return Collections.singletonList(vertex);
            }, // Person.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long loanID = Long.parseLong(fields[0]);
                final double loanAmount = Double.parseDouble(fields[1]);
                final RCase4Vertex case4Vertex = new RCase4Vertex(VertexType.Loan, loanID, loanAmount, new HashMap<>());
                // use `gID ~ globalID` as the index
                final long gID = globalID(VertexType.Loan, loanID);
                final IVertex<Long, RCase4Vertex> vertex = new ValueVertex<>(gID, case4Vertex);
                return Collections.singletonList(vertex);
            }, // Loan.csv
    };
    public static final FileSource.FileLineParser<IEdge<Long, Null>>[] edgeParsers = new FileSource.FileLineParser[]{
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long fromPersonID = Long.parseLong(fields[0]);
                final long toPersonID = Long.parseLong(fields[1]);
                // use `gID ~ globalID` as the index
                final long gid_from = globalID(VertexType.Person, fromPersonID);
                final long gid_to = globalID(VertexType.Person, toPersonID);
                // invert the edge direction while loading
                final IEdge<Long, Null> edge = new ValueEdge<>(gid_to, gid_from, new Null());
                return Collections.singletonList(edge);
            }, // PersonGuaranteePerson.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long fromPersonID = Long.parseLong(fields[0]);
                final long toLoanID = Long.parseLong(fields[1]);
                // use `gID ~ globalID` as the index
                final long gid_from = globalID(VertexType.Person, fromPersonID);
                final long gid_to = globalID(VertexType.Loan, toLoanID);
                // invert the edge direction while loading
                final IEdge<Long, Null> edge = new ValueEdge<>(gid_to, gid_from, new Null());
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
            final PWindowSource<IVertex<Long, RCase4Vertex>> vertices = pipelineTaskCtx.buildSource(
                    new DataSource<>(vertexFilePaths, vertexParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Null>> edges = pipelineTaskCtx.buildSource(
                    new DataSource<>(edgeFilePaths, edgeParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("RCase4Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            final PGraphWindow<Long, RCase4Vertex, Null> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<Case4Cell> orderedSink = new OrderedFileSink<>("result4.csv");

            graphWindow.compute(new RCase4Algorithm(5))
                    .compute(Env.PARALLELISM_MAX)
                    .getVertices()
                    .filter(vertex -> !vertex.getValue().getReceivedPersonLoanAmountMap().isEmpty())
                    .map(vertex -> vertex.getValue().toCase4Cell())
                    .sink(orderedSink)
                    .withParallelism(Env.SINK_PARALLELISM_MAX);
        });

        return pipeline.execute();
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            ABSOLUTE_PREFIX = args[0];
            RESULT_FILE_PATH = args[1];
        }
        LOGGER.info("*** Start R-Case4 ***");
        final Environment environment = EnvironmentUtil.loadEnvironment(new String[]{});
        final IPipelineResult<?> result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
        LOGGER.info("*** End R-Case4 ***");
    }
}