package org.ember.TuGraphFinbench.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.ember.TuGraphFinbench.DataLoader;
import org.ember.TuGraphFinbench.Record.Edge;
import org.ember.TuGraphFinbench.Record.RawEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;

public class TinyEdgeBuilder extends RichFunction implements SourceFunction<IEdge<Long, Double>> {

    static final Logger LOGGER = LoggerFactory.getLogger(VertexBuilder.class);
    protected transient RuntimeContext runtimeContext;
    protected List<IEdge<Long, Double>> records;
    protected Integer readPos = null;

    public static List<IEdge<Long, Double>> buildEdges() {
        return DataLoader.loadEdges().stream().map(RawEdge::into).map((Edge edge) -> {
            return new ValueEdge<>(edge.getSrcCodec(), edge.getDstCodec(), edge.getAmount());
        }).collect(Collectors.toList());
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void init(int parallel, int index) {
        this.records = buildEdges();
        if (parallel > 1) {
            List<IEdge<Long, Double>> allRecords = this.records;
            this.records = new ArrayList<>();
            for (int i = index; i < allRecords.size(); i += parallel) {
                this.records.add(allRecords.get(i));
            }
        }
    }

    @Override
    public boolean fetch(IWindow<IEdge<Long, Double>> window, SourceContext<IEdge<Long, Double>> ctx) throws Exception {
        LOGGER.info(
                "Fetching.Edge(taskID: {}, batchID: {}).Start(ReadPos: {}) ... With(totalSize: {})",
                this.runtimeContext.getTaskArgs().getTaskId(),
                window.windowId(),
                readPos,
                records.size());

        if (readPos == null) {
            readPos = 0;
        }

        while (readPos < records.size()) {
            IEdge<Long, Double> out = records.get(readPos);
            long windowId = window.assignWindow(out);
            if (window.windowId() == windowId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }

        boolean result = readPos < records.size();

        LOGGER.info("Fetching.Edge(batchID: {}).Current(ReadPos: {}).Result = {}",
                window.windowId(),
                readPos,
                result);

        return result;
    }

    @Override
    public void close() {
    }
}
