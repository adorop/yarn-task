package com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv;

import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.Deserializer;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;

import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class DefaultCsvDeserializer implements Deserializer {

    private final List<Field> dataFields;
    private final List<Integer> cellsIndexes;

    public DefaultCsvDeserializer(Schema schema) {
        dataFields = schema.getFields();
        cellsIndexes = IntStream.range(0, dataFields.size())
                .boxed()
                .collect(toList());
    }

    public DefaultCsvDeserializer(Schema dataSchema,
                                  Schema selectSchema) {
        dataFields = dataSchema.getFields();
        List<Field> selectFields = selectSchema.getFields();
        cellsIndexes = IntStream.range(0, dataFields.size())
                .filter(i -> selectFields.contains(dataFields.get(i)))
                .boxed()
                .collect(toList());
    }

    @Override
    public SerDeResult<Record> apply(String line) {
        try {
            String[] values = line.split(CsvProperties.DEFAULT_SEPARATOR);
            List<Object> cells = cellsIndexes.stream()
                    .map(i -> parse(values[i], dataFields.get(i)))
                    .collect(toList());
            return SerDeResult.successful(new Record(cells));
        } catch (Exception e) {
            return SerDeResult.failed(new SerDeResult.Failure(line, e.getMessage()));
        }
    }

    private Object parse(String value, Field field) {
        Function<String, Object> parser = field.getFromStringParser();
        return parser.apply(value.trim());
    }
}
