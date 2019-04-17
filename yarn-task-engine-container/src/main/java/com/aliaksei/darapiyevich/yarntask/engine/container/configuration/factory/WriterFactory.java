package com.aliaksei.darapiyevich.yarntask.engine.container.configuration.factory;

import com.aliaksei.darapiyevich.yarntask.engine.container.Writer;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv.DefaultCsvSerializer;
import com.aliaksei.darapiyevich.yarntask.engine.container.writer.FileSystemWriter;
import com.aliaksei.darapiyevich.yarntask.engine.container.writer.HashPartitioner;
import com.aliaksei.darapiyevich.yarntask.engine.container.writer.PartitionedWriter;
import com.aliaksei.darapiyevich.yarntask.engine.container.writer.Partitioner;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.PartitionsDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.WriterDefinition;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class WriterFactory {
    private final FileSystem fileSystem;

    public Writer create(TaskDefinition taskDefinition) {

        WriterDefinition writerDefinition = taskDefinition.getWriterDefinition();
        return writerDefinition.getSinglePath()
                .map(path -> getSimpleWriter(fileSystem, path))
                .orElseGet(() -> getPartitionedWriter(writerDefinition.getPartitionsDefinition().get(), fileSystem));
    }

    private Writer getSimpleWriter(FileSystem fileSystem, String path) {
        return new FileSystemWriter(fileSystem, new Path(path), new DefaultCsvSerializer());
    }

    private PartitionedWriter getPartitionedWriter(PartitionsDefinition partitionsDefinition, FileSystem fileSystem) {
        Partitioner partitioner = new HashPartitioner(partitionsDefinition.getSchema(),
                partitionsDefinition.getPaths().size(),
                partitionsDefinition.getPartitionByKeys());
        List<Writer> partitionsHandlers = partitionsDefinition.getPaths().stream()
                .map(path -> getSimpleWriter(fileSystem, path))
                .collect(toList());
        return new PartitionedWriter(partitioner, partitionsHandlers);
    }
}
