package org.ember.TuGraphFinbench.Source;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import org.datanucleus.store.types.wrappers.List;

public abstract class BaseSource<OUT> extends RichFunction implements SourceFunction<OUT> {

    protected transient RuntimeContext runtimeContext;
    protected Integer readPos = null;
    protected List<OUT> records;

    @Override
    public abstract void init(int parallel, int index);

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
        return readPos < records.size();
    }

    @Override
    public void close() {
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

}
