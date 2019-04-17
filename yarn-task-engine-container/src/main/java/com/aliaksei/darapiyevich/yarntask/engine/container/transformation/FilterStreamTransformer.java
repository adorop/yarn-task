package com.aliaksei.darapiyevich.yarntask.engine.container.transformation;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.predicate.Predicate;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FilterStreamTransformer implements StreamTransformer {
    private final BiPredicate<Object, Object> operatorFunction;
    private final Object valueToCompareTo;
    private final int columnIndex;

    public FilterStreamTransformer(Schema schema,
                                   Predicate predicate) {
        this.valueToCompareTo = predicate.getValue();
        this.operatorFunction = predicate.getFunction();
        this.columnIndex = getColumnIndex(schema.getFields(), predicate.getColumn());
    }

    private int getColumnIndex(List<Field> fields, String column) {
        return IntStream.range(0, fields.size())
                .filter(i -> fields.get(i).getName().equals(column))
                .findAny()
                .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public Stream<Record> apply(Stream<Record> stream) {
        return stream.filter(record -> {
                    Object cellValue = record.getCells().get(columnIndex);
                    return operatorFunction.test(cellValue, valueToCompareTo);
                }
        );
    }
}
