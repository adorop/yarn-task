package com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv;

import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.Serializer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;

import static com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv.CsvProperties.DEFAULT_LINE_BREAK;
import static com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv.CsvProperties.DEFAULT_SEPARATOR;
import static java.util.stream.Collectors.joining;

public class DefaultCsvSerializer implements Serializer {

    private static final String EMPTY_PREFIX = "";

    @Override
    public SerDeResult<byte[]> serialize(Record record) {
        byte[] result = record.getCells().stream()
                .map(Object::toString)
                .collect(joining(DEFAULT_SEPARATOR, EMPTY_PREFIX, DEFAULT_LINE_BREAK))
                .getBytes();
        return SerDeResult.successful(result);
    }
}
