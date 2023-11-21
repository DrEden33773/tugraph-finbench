package org.ember.TuGraphFinbench.Source;

import java.util.ArrayList;
import java.util.List;

import org.ember.TuGraphFinbench.Builder.VertexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;

public class DataSource<OUT> extends RichFunction implements SourceFunction<OUT> {

    static final Logger LOGGER = LoggerFactory.getLogger(VertexBuilder.class);
    protected transient RuntimeContext runtimeContext;
    protected List<OUT> records;

    public DataSource(List<OUT> records) {
        this.records = records;
    }

    protected Integer readPos = null;

    @Override
    public void init(int parallel, int index) {
        if (parallel > 1) {
            List<OUT> allRecords = this.records;
            this.records = new ArrayList<>();
            for (int i = index; i < allRecords.size(); i += parallel) {
                this.records.add(allRecords.get(i));
            }
        }
    }

    @Override
    public boolean fetch(IWindow<OUT> window, SourceContext<OUT> ctx) throws Exception {
        if (readPos == null) {
            readPos = 0;
        }

        while (readPos < records.size()) {
            OUT out = records.get(readPos);
            long windowId = window.assignWindow(out);
            if (window.windowId() == windowId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }

        boolean result = readPos < records.size();

        return result;
    }

    @Override
    public void close() {
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

}
