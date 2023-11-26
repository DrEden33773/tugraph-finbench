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
import org.ember.TuGraphFinbench.Algorithms.Case1Algorithm;
import org.ember.TuGraphFinbench.Cell.Case1Cell;
import org.ember.TuGraphFinbench.Env.Env;
import org.ember.TuGraphFinbench.Record.Case1Vertex;
import org.ember.TuGraphFinbench.Record.VertexType;
import org.ember.TuGraphFinbench.Sink.OrderedFileSink;
import org.ember.TuGraphFinbench.Source.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class Case1 {

    public static final Null nullEdgeProperty = new Null();

    public static final Logger LOGGER = LoggerFactory.getLogger(Case1.class);
    public static final String[] vertexFilePaths = {"Person.csv", "Account.csv", "Loan.csv"};
    public static final String[] edgeFilePaths = {"PersonOwnAccount.csv", "AccountTransferAccount.csv", "LoanDepositAccount.csv"};
    public static long globalID(VertexType t, long originID) { 
      long flag = 0;
      if (t == VertexType.Person) {
        flag = 1;
      }
      if (t == VertexType.Account) {
        flag = 2;
      }
      if (t == VertexType.Loan) {
        flag = 3;
      }
      long newID = (originID << 2) | flag;
      return newID;
    }
    public static final FileLineParser<IVertex<Long, Case1Vertex>>[] vertexParsers = new FileLineParser[]{
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long personID = Long.parseLong(fields[0]);
                final Case1Vertex case1Vertex = new Case1Vertex(VertexType.Person, personID, 0, 0);
                final long gID = globalID(VertexType.Person, personID);
                final IVertex<Long, Case1Vertex> vertex = new ValueVertex<>(gID, case1Vertex);
                return Collections.singletonList(vertex);
            }, // Person.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long accountID = Long.parseLong(fields[0]);
                final Case1Vertex case1Vertex = new Case1Vertex(VertexType.Account, accountID, 0, 0);
                final long gID = globalID(VertexType.Account, accountID);
                final IVertex<Long, Case1Vertex> vertex = new ValueVertex<>(gID, case1Vertex);
                return Collections.singletonList(vertex);
            }, // Account.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long loanID = Long.parseLong(fields[0]);
                final double loanAmount = Double.parseDouble(fields[1]);
                final long gID = globalID(VertexType.Loan, loanID);
                final Case1Vertex case1Vertex = new Case1Vertex(VertexType.Loan, loanID, loanAmount, 0);
                final IVertex<Long, Case1Vertex> vertex = new ValueVertex<>(gID, case1Vertex);
                return Collections.singletonList(vertex);
            }, // Loan.csv
    };
    public static final FileLineParser<IEdge<Long, Null>>[] edgeParsers = new FileLineParser[]{
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long fromPersonID = Long.parseLong(fields[0]);
                final long toAccountID = Long.parseLong(fields[1]);
                final long gid_from = globalID(VertexType.Person, fromPersonID);
                final long gid_to = globalID(VertexType.Account, toAccountID);
                // invert the edge direction while loading
                final IEdge<Long, Null> edge = new ValueEdge<>(gid_to, gid_from, nullEdgeProperty);
                return Collections.singletonList(edge);
            }, // PersonOwnAccount.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long fromAccountID = Long.parseLong(fields[0]);
                final long toAccountID = Long.parseLong(fields[1]);
                final long gid_from = globalID(VertexType.Account, fromAccountID);
                final long gid_to = globalID(VertexType.Account, toAccountID);
                //final IEdge<Long, Null> edge = new ValueEdge<>(fromAccountID, toAccountID, new Null());
                final IEdge<Long, Null> edge = new ValueEdge<>(gid_from, gid_to, nullEdgeProperty);
                return Collections.singletonList(edge);
            }, // AccountTransferAccount.csv
            (final String line) -> {
                final String[] fields = line.split("\\|");
                final long fromLoanID = Long.parseLong(fields[0]);
                final long toAccountID = Long.parseLong(fields[1]);
                long gid_from = globalID(VertexType.Loan, fromLoanID);
                long gid_to = globalID(VertexType.Account, toAccountID);
                final IEdge<Long, Null> edge = new ValueEdge<>(gid_from, gid_to, nullEdgeProperty);
                return Collections.singletonList(edge);
            }, // LoanDepositAccount.csv
    };
    public static String RESULT_FILE_PATH = "./target/tmp/data/result/finbench";
    public static String ABSOLUTE_PREFIX = null;

    private static IPipelineResult<?> submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        final Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCtx -> {
            final PWindowSource<IVertex<Long, Case1Vertex>> vertices = pipelineTaskCtx.buildSource(
                    new DataSource<>(vertexFilePaths, vertexParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final PWindowSource<IEdge<Long, Null>> edges = pipelineTaskCtx.buildSource(
                    new DataSource<>(edgeFilePaths, edgeParsers, ABSOLUTE_PREFIX), AllWindow.getInstance()
            ).withParallelism(Env.PARALLELISM_MAX);

            final GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView("Case1Graph")
                    .withShardNum(Env.PARALLELISM_MAX)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            final PGraphWindow<Long, Case1Vertex, Null> graphWindow = pipelineTaskCtx.buildWindowStreamGraph(vertices,
                    edges, graphViewDesc);

            final SinkFunction<Case1Cell> orderedSink = new OrderedFileSink<>("result1.csv");

            graphWindow.compute(new Case1Algorithm(4))
                    .compute(Env.PARALLELISM_MAX)
                    .getVertices()
                    .filter(vertex -> vertex.getValue().getNthLayer() == 4)
                    .map(vertex -> vertex.getValue().toCase1Cell())
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
        LOGGER.info("*** Start Case1 ***");
        final Environment environment = EnvironmentUtil.loadEnvironment(new String[]{});
        final IPipelineResult<?> result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
        LOGGER.info("*** End Case1 ***");
    }
}
