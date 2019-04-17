package com.aliaksei.darapiyevich.yarntask.engine.container.transformation.aggregate;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.SchemaUtils;
import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

public class AggregateCollector<ACCUMULATOR, VALUE> implements Collector<Record, Map<Record, ACCUMULATOR>, Stream<Record>> {
    private final Supplier<ACCUMULATOR> initialValueSupplier;
    private final BiConsumer<ACCUMULATOR, VALUE> accumulator;
    private final Function<ACCUMULATOR, Object> finisher;
    private final List<Integer> groupByColumnsIndexes;
    private final Integer aggregationOperandIndex;

    public AggregateCollector(Supplier<ACCUMULATOR> initialValueSupplier,
                              String aggregationOperandColumn,
                              BiConsumer<ACCUMULATOR, VALUE> accumulator,
                              Function<ACCUMULATOR, Object> finisher,
                              List<String> groupByColumns,
                              Schema schema) {
        this.initialValueSupplier = initialValueSupplier;
        this.accumulator = accumulator;
        this.finisher = finisher;
        groupByColumnsIndexes = SchemaUtils.getCellIndexes(schema, groupByColumns);
        aggregationOperandIndex = ofNullable(aggregationOperandColumn)
                .map(column -> SchemaUtils.getCellIndex(schema, column))
                .orElse(null);
    }

    @Override
    public Supplier<Map<Record, ACCUMULATOR>> supplier() {
        return HashMap::new;
    }

    @Override
    public BiConsumer<Map<Record, ACCUMULATOR>, Record> accumulator() {
        return (groupByKeyToAccumulator, record) -> {
            Record groupByKey = getGroupByKey(record);
            ACCUMULATOR accumulator = groupByKeyToAccumulator
                    .computeIfAbsent(groupByKey, r -> initialValueSupplier.get());
            this.accumulator.accept(accumulator, getValue(record));
        };
    }

    private Record getGroupByKey(Record record) {
        List<Object> cells = record.getCells();
        List<Object> cellsToAggregateBy = groupByColumnsIndexes.stream()
                .map(cells::get)
                .collect(toList());
        return new Record(cellsToAggregateBy);
    }

    @SuppressWarnings("unchecked")
    private VALUE getValue(Record record) {
        return (VALUE) ofNullable(aggregationOperandIndex)
                .map(i -> record.getCells().get(i))
                .orElse(null);
    }

    @Override
    public BinaryOperator<Map<Record, ACCUMULATOR>> combiner() {
        return null;
    }

    @Override
    public Function<Map<Record, ACCUMULATOR>, Stream<Record>> finisher() {
        return groupByKeyToAccumulator -> groupByKeyToAccumulator.entrySet().stream()
                .map(entry -> new Record(merge(entry.getKey(), entry.getValue())));
    }

    private List<Object> merge(Record groupByKey, ACCUMULATOR accumulator) {
        return ImmutableList.builder()
                .addAll(groupByKey.getCells())
                .add(finisher.apply(accumulator))
                .build();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}
