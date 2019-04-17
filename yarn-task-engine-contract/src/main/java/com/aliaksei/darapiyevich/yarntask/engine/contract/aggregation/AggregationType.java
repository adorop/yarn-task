package com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface AggregationType<ACCUMULATOR, VALUE> {
    Supplier<ACCUMULATOR> initialValueSupplier();
    BiConsumer<ACCUMULATOR, VALUE> accumulator();
    Function<ACCUMULATOR, Object> finisher();
    Type aggregationFieldType();
    String name();
}
