package com.aliaksei.darapiyevich.yarntask.engine.container.writer;

import com.aliaksei.darapiyevich.yarntask.engine.container.Writer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.util.List;

@RequiredArgsConstructor
public class PartitionedWriter implements Writer {
    private final Partitioner partitioner;
    private final List<Writer> partitionsHandlers;

    @Override
    public void init() {
        partitionsHandlers.forEach(Writer::init);
    }

    @Override
    public void write(Record element) {
        int partition = partitioner.getPartition(element);
        partitionsHandlers.get(partition)
                .write(element);
    }

    @Override
    public void close() throws Exception {
        partitionsHandlers.forEach(handler -> close(handler));
    }

    @SneakyThrows
    private void close(Writer handler) {
        handler.close();
    }
}
