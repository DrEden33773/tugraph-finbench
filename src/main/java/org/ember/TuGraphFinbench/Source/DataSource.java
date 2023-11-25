package org.ember.TuGraphFinbench.Source;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.example.function.FileSource.FileLineParser;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DataSource<OUT> extends RichFunction implements SourceFunction<OUT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSource.class);

    protected final String[] filePaths;
    protected final FileLineParser<OUT>[] parsers;
    protected List<OUT> records = new ArrayList<>();
    protected Integer readPos = null;
    protected String absolutePrefix = null;
    protected transient RuntimeContext runtimeContext;

    public DataSource(String[] filePaths, FileLineParser<OUT>[] parsers) {
        this.filePaths = filePaths;
        this.parsers = parsers;
    }

    public DataSource(String[] filePaths, FileLineParser<OUT>[] parsers, String absolutePrefix) {
        this.filePaths = filePaths;
        this.parsers = parsers;
        this.absolutePrefix = absolutePrefix;
    }

    private List<OUT> readFilelines(final String filePath, final FileLineParser<OUT> parser) {
        try {
            final List<String> lines = absolutePrefix == null
                    ? Resources.readLines(Resources.getResource(filePath), Charset.defaultCharset())
                    : Files.readAllLines(Paths.get(absolutePrefix + filePath), Charset.defaultCharset());
            final List<OUT> result = new ArrayList<>();
            boolean isHeader = true;
            for (final String line : lines) {
                if (isHeader) {
                    isHeader = false;
                } else {
                    result.addAll(parser.parse(line));
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("error in read resource file: " + filePath, e);
        }
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void init(int parallel, int index) {
        // read
        assert parsers.length == filePaths.length;
        for (int i = 0; i < filePaths.length; i++) {
            this.records.addAll(readFilelines(filePaths[i], parsers[i]));
        }
        // parallel
        List<OUT> allRecords = records;
        records = new ArrayList<>();
        for (int i = 0; i < allRecords.size(); i++) {
            if (i % parallel == index) {
                records.add(allRecords.get(i));
            }
        }
    }

    @Override
    public boolean fetch(IWindow<OUT> window, SourceContext<OUT> ctx) throws Exception {
        LOGGER.info("collection source fetch taskId:{}, batchId:{}, start readPos {}, totalSize {}",
                runtimeContext.getTaskArgs().getTaskId(), window.windowId(), readPos, records.size());
        readPos = readPos == null ? 0 : readPos;
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
        final boolean result = readPos < records.size();
        LOGGER.info("collection source fetch batchId:{}, current readPos {}, result {}",
                window.windowId(), readPos, result);
        return result;
    }

    @Override
    public void close() {
    }
}
