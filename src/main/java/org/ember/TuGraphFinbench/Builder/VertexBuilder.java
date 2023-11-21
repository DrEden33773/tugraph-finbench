package org.ember.TuGraphFinbench.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.ember.TuGraphFinbench.DataLoader;
import org.ember.TuGraphFinbench.Record.Vertex;
import org.ember.TuGraphFinbench.Record.RawVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;

public class VertexBuilder extends RichFunction implements SourceFunction<IVertex<Long, Vertex>> {

    static final Logger LOGGER = LoggerFactory.getLogger(VertexBuilder.class);
    protected transient RuntimeContext runtimeContext;
    protected List<IVertex<Long, Vertex>> records;
    protected Integer readPos = null;

    public static List<IVertex<Long, Vertex>> buildVertices() {
        return DataLoader.loadNodes().stream().map(RawVertex::into).map((Vertex node) -> {
            return new ValueVertex<>(node.getID(), node);
        }).collect(Collectors.toList());
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void init(int parallel, int index) {
        this.records = buildVertices();
        if (parallel > 1) {
            List<IVertex<Long, Vertex>> allRecords = this.records;
            this.records = new ArrayList<>();
            for (int i = index; i < allRecords.size(); i += parallel) {
                this.records.add(allRecords.get(i));
            }
        }
    }

    @Override
    public boolean fetch(IWindow<IVertex<Long, Vertex>> window, SourceContext<IVertex<Long, Vertex>> ctx)
            throws Exception {
        LOGGER.info(
                "Fetching.Node(taskID: {}, batchID: {}).Start(ReadPos: {}) ... With(totalSize: {})",
                this.runtimeContext.getTaskArgs().getTaskId(),
                window.windowId(),
                readPos,
                records.size());

        if (readPos == null) {
            readPos = 0;
        }

        while (readPos < records.size()) {
            IVertex<Long, Vertex> out = records.get(readPos);
            long windowId = window.assignWindow(out);
            if (window.windowId() == windowId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }

        boolean result = readPos < records.size();

        LOGGER.info("Fetching.Node(batchID: {}).Current(ReadPos: {}).Result = {}",
                window.windowId(),
                readPos,
                result);

        return result;
    }

    @Override
    public void close() {
    }
}
