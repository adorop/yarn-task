package com.aliaksei.darapiyevich.yarntask.engine.container;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.util.List;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class Task {
    private final List<StreamReader> streamReaders;
    private final StreamTransformer streamTransformer;
    private final Writer writer;

    @SneakyThrows
    public void execute() {
        try (Stream<Record> inputStream = concatStreams()) {
            writer.init();
            streamTransformer.apply(inputStream)
                    .forEach(writer::write);
        } finally {
            writer.close();
        }
    }

    private Stream<Record> concatStreams() {
        return streamReaders.stream()
                .map(StreamReader::get)
                .reduce(Stream::concat)
                .orElseGet(Stream::empty);
    }
}
