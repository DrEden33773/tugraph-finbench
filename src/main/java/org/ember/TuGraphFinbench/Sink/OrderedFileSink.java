package org.ember.TuGraphFinbench.Sink;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.PriorityQueue;

public class OrderedFileSink<OUT extends Comparable<OUT>> extends RichFunction implements SinkFunction<OUT> {

    public static final String OUTPUT_DIR = "output.dir";
    public static final String FILE_OUTPUT_APPEND_ENABLE = "file.append.enable";
    public static final String header = "id|value";
    static final Logger LOGGER = LoggerFactory.getLogger(OrderedFileSink.class);
    File file;
    PriorityQueue<OUT> queue = new PriorityQueue<OUT>();

    public OrderedFileSink() {
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        String filePath = String.format("%s/result_%s",
                runtimeContext.getConfiguration().getString(OUTPUT_DIR), runtimeContext.getTaskArgs().getTaskIndex());
        LOGGER.info("sink file name {}", filePath);
        boolean append = runtimeContext.getConfiguration().getBoolean(new ConfigKey(FILE_OUTPUT_APPEND_ENABLE, true));
        file = new File(filePath);
        try {
            if (!append && file.exists()) {
                try {
                    FileUtils.forceDelete(file);
                } catch (Exception e) {
                    // ignore
                }
            }
            if (!file.exists()) {
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                file.createNewFile();
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void close() {
        // Now, write data in `queue` to file
        try {
            FileUtils.write(file, header + "\n", Charset.defaultCharset(), true);
            while (!queue.isEmpty()) {
                FileUtils.write(file, queue.poll().toString() + "\n", Charset.defaultCharset(), true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(OUT out) throws Exception {
        // simply write data into `queue`
        queue.add(out);
    }
}
