package com.aliaksei.darapiyevich.yarntask.engine.container.writer;

import com.aliaksei.darapiyevich.yarntask.engine.container.Writer;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.Serializer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@RequiredArgsConstructor
@Slf4j
public class FileSystemWriter implements Writer {
    static final boolean RECURSIVELY = true;
    static final boolean OVERWRITE = true;

    private final FileSystem fileSystem;
    private final Path outputPath;
    private final Serializer outputSerializer;

    private FSDataOutputStream fsDataOutputStream;

    @Override
    @SneakyThrows
    public void init() {
        fsDataOutputStream = fileSystem.create(outputPath, OVERWRITE);
    }

    @Override
    @SneakyThrows
    public void write(Record element) {
        SerDeResult<byte[]> serialized = outputSerializer.serialize(element);
        if (serialized.isSuccessful()) {
            fsDataOutputStream.write(serialized.getResult());
        } else {
            log.info("Serialization failure: {}", serialized.getFailure());
        }
    }

    @Override
    public void close() throws Exception {
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }
}
