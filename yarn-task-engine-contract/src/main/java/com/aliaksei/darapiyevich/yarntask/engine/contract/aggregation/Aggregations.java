package com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation;

import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.PrimitiveType;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class Aggregations {
    private static final List<AggregationType<?, ?>> registry = Arrays.asList(
            new CountAggregationType(),
            new SumAggregationType()
    );

    public static Aggregation.AggregationBuilder<?, ?> of(String aggregationTypeName, String aggregationOperandColumn) {
        return new Aggregation.AggregationBuilder<>(registry.stream()
                .filter(aggregationType -> aggregationType.name().equals(aggregationTypeName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Unknown aggregation type")), aggregationOperandColumn);
    }

    public static Aggregation.AggregationBuilder<MutableLong, Long> count() {
        return new Aggregation.AggregationBuilder<>(new CountAggregationType(), null);
    }

    public static Aggregation.AggregationBuilder<MutableLong, Number> sum(String column) {
        return new Aggregation.AggregationBuilder<>(new SumAggregationType(), column);
    }

    private static class CountAggregationType implements AggregationType<MutableLong, Long> {

        @Override
        public Supplier<MutableLong> initialValueSupplier() {
            return () -> new MutableLong(0L);
        }

        @Override
        public BiConsumer<MutableLong, Long> accumulator() {
            return (mutableLong, record) -> mutableLong.increment();
        }

        @Override
        public Function<MutableLong, Object> finisher() {
            return MutableLong::longValue;
        }

        @Override
        public Type aggregationFieldType() {
            return PrimitiveType.LONG;
        }

        @Override
        public String name() {
            return "count";
        }
    }

    @RequiredArgsConstructor
    private static class SumAggregationType implements AggregationType<MutableLong, Number> {

        @Override
        public Supplier<MutableLong> initialValueSupplier() {
            return () -> new MutableLong(0);
        }

        @Override
        public BiConsumer<MutableLong, Number> accumulator() {
            return MutableLong::add;
        }

        @Override
        public Function<MutableLong, Object> finisher() {
            return MutableLong::longValue;
        }

        @Override
        public Type aggregationFieldType() {
            return PrimitiveType.LONG;
        }

        @Override
        public String name() {
            return "sum";
        }
    }
}
