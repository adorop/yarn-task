package com.aliaksei.darapiyevich.yarntask.engine.container.configuration.factory;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamReader;
import com.aliaksei.darapiyevich.yarntask.engine.container.reader.FileReader;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv.DefaultCsvDeserializer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class StreamReadersFactory {
    private final FileSystem fileSystem;

    public List<StreamReader> create(TaskDefinition taskDefinition) {
        ReaderDefinition readerDefinition = taskDefinition.getReaderDefinition();
        return readerDefinition.getPaths().stream()
                .map(Path::new)
                .map(path -> new FileReader(fileSystem, path, getDeserializer(readerDefinition)))
                .collect(toList());
    }

    private DefaultCsvDeserializer getDeserializer(ReaderDefinition readerDefinition) {
        Optional<Schema> selectSchema = readerDefinition.getSelectSchema();
        return selectSchema
                .map(schema -> new DefaultCsvDeserializer(readerDefinition.getDataSchema(), schema))
                .orElseGet(() -> new DefaultCsvDeserializer(readerDefinition.getDataSchema()));
    }
}
