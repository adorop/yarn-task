package com.aliaksei.darapiyevich.yarntask.engine.container.transformation;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class StreamTransformerComposite implements StreamTransformer {
    private final List<StreamTransformer> components;

    @Override
    public Stream<Record> apply(Stream<Record> stream) {
        for (StreamTransformer component : components) {
            stream = component.apply(stream);
        }
        return stream;
    }
}
