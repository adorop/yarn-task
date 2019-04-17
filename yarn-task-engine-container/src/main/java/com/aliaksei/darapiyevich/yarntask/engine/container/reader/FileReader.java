package com.aliaksei.darapiyevich.yarntask.engine.container.reader;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamReader;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.Deserializer;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

@RequiredArgsConstructor
@Slf4j
public class FileReader implements StreamReader {
    private final FileSystem fileSystem;
    private final Path file;
    private final Deserializer deserializer;

    private BufferedReader bufferedReader;

    @Override
    @SneakyThrows
    public Stream<Record> get() {
        InputStream inputStream = getInputStream();
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        Stream<String> lines = bufferedReader.lines();
        return lines.map(deserializer)
                .peek(this::logFailure)
                .filter(SerDeResult::isSuccessful)
                .map(SerDeResult::getResult);
    }

    private InputStream getInputStream() throws IOException {
        InputStream inputStream = openDataStream();
        return isGzipCompressed() ? wrapInGzipped(inputStream) : inputStream;
    }

    private boolean isGzipCompressed() {
        return file.getName().endsWith(".gz");
    }

    @VisibleForTesting
    InputStream openDataStream() throws IOException {
        return fileSystem.open(file);
    }

    @VisibleForTesting
    InputStream wrapInGzipped(InputStream inputStream) throws IOException {
        return new GZIPInputStream(inputStream);
    }

    private void logFailure(SerDeResult result) {
        if (!result.isSuccessful()) {
            log.info("Deserialization failure: {}", result.getFailure());
        }
    }

    @Override
    public void close() throws Exception {
        if (bufferedReader != null) {
            bufferedReader.close();
        }
    }
}
