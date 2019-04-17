package com.aliaksei.darapiyevich.yarntask.engine.contract.schema;

import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class SchemaUtils {

    public static List<Integer> getCellIndexes(Schema schema, List<String> columns) {
        List<Field> fields = schema.getFields();
        return IntStream.range(0, fields.size())
                .filter(i -> columns.contains(fields.get(i).getName()))
                .boxed()
                .collect(toList());
    }

    public static Integer getCellIndex(Schema schema, String column) {
        List<Field> fields = schema.getFields();
        return IntStream.range(0, fields.size())
                .filter(i -> fields.get(i).getName().equals(column))
                .findAny()
                .orElseThrow(IllegalArgumentException::new);
    }

    public static List<Field> selectFields(Schema schema, String... fieldsNames) {
        List<Field> fields = schema.getFields();
        return getCellIndexes(schema, Arrays.asList(fieldsNames)).stream()
                .map(fields::get)
                .collect(toList());
    }
}
