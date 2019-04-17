package com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation;

import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Getter
public class Aggregation<ACCUMULATOR, VALUE> {
    private Supplier<ACCUMULATOR> initialValueSupplier;
    private BiConsumer<ACCUMULATOR, VALUE> accumulator;
    private Function<ACCUMULATOR, Object> finisher;
    private String type;
    private List<String> columns;
    private String aggregationOperandColumn;
    private String alias;
    private Type newFieldType;

    @RequiredArgsConstructor
    public static class AggregationBuilder<ACCUMULATOR, VALUE> {
        private final AggregationType<ACCUMULATOR, VALUE> type;
        private final String aggregationOperandColumn;
        private List<String> columns;

        public AggregationBuilder<ACCUMULATOR, VALUE> by(String... columns) {
            this.columns = Arrays.asList(columns);
            return this;
        }

        public AggregationBuilder<ACCUMULATOR, VALUE> by(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Aggregation<ACCUMULATOR, VALUE> as(String alias) {
            Aggregation<ACCUMULATOR, VALUE> aggregation = new Aggregation<>();
            aggregation.initialValueSupplier = type.initialValueSupplier();
            aggregation.accumulator = type.accumulator();
            aggregation.finisher = type.finisher();
            aggregation.type = type.name();
            aggregation.newFieldType = type.aggregationFieldType();
            aggregation.columns = columns;
            aggregation.aggregationOperandColumn = aggregationOperandColumn;
            aggregation.alias = alias;
            return aggregation;
        }
    }
}
