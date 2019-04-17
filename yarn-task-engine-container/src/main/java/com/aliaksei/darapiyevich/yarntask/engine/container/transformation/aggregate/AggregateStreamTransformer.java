package com.aliaksei.darapiyevich.yarntask.engine.container.transformation.aggregate;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.RequiredArgsConstructor;

import java.util.stream.Stream;

@RequiredArgsConstructor
public class AggregateStreamTransformer<A, V> implements StreamTransformer {
    private final Aggregation<A, V> aggregation;
    private final Schema schema;

    @Override
    public Stream<Record> apply(Stream<Record> stream) {
        return stream.collect(new AggregateCollector<>(
                aggregation.getInitialValueSupplier(),
                aggregation.getAggregationOperandColumn(),
                aggregation.getAccumulator(),
                aggregation.getFinisher(),
                aggregation.getColumns(),
                schema
        ));
    }
}
