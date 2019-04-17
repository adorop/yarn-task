package com.aliaksei.darapiyevich.yarntask.engine.container.transformation.aggregate;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AggregateCollectorTest {
    private static final int INITIAL_VALUE = 0;
    private static final Integer EXISTING_VALUE = 3;
    private static final String FIRST_GROUP_BY_VALUE = "firstGroupByValue";
    private static final String SECOND_GROUP_BY_VALUE = "secondGroupByValue";
    private static final String AGGREGATE_FIELD = "aggregateField";
    private static final String AGGREGATE_VALUE = "aggregateValue";

    private final List<String> groupByColumns = Arrays.asList("group", "by");
    private final Record record = new Record(Arrays.asList(FIRST_GROUP_BY_VALUE, AGGREGATE_VALUE, SECOND_GROUP_BY_VALUE));

    @Mock
    private Supplier<Integer> initialValueSupplier;
    @Mock
    private BiConsumer<Integer, String> accumulator;
    @Mock
    private Function<Integer, Object> finisher;
    @Mock
    private Type type;

    private AggregateCollector<Integer, String> collector;

    @Before
    public void setUp() throws Exception {
        initInitialValue();
        collector = new AggregateCollector<>(initialValueSupplier, AGGREGATE_FIELD, accumulator, finisher, groupByColumns, getSchema());
    }

    private void initInitialValue() {
        when(initialValueSupplier.get()).thenReturn(INITIAL_VALUE);
    }

    private Schema getSchema() {
        return Schema.builder()
                .field(new Field("group", type))
                .field(new Field(AGGREGATE_FIELD, type))
                .field(new Field("by", type))
                .build();
    }

    @Test
    public void accumulatorShouldApplyToInitialValueWhenGivenKeyDoesNotExist() {
        collector.accumulator().accept(new HashMap<>(), record);
        verify(accumulator).accept(INITIAL_VALUE, AGGREGATE_VALUE);
    }

    @Test
    public void accumulatorShouldApplyToValueInMapWhenExists() {
        List<Object> existingKey = Arrays.asList(FIRST_GROUP_BY_VALUE, SECOND_GROUP_BY_VALUE);
        collector.accumulator()
                .accept(singletonMap(new Record(existingKey), EXISTING_VALUE), record);
        verify(accumulator).accept(EXISTING_VALUE, AGGREGATE_VALUE);
    }

    private Map<Record, Integer> singletonMap(Record record, Integer value) {
        HashMap<Record, Integer> map = new HashMap<>(1);
        map.put(record, value);
        return map;
    }

    @Test
    public void finisherShouldAppendFinalValueToGroupByKey() {
        String finalValue = getFinalValue(EXISTING_VALUE);
        Record result = collector.finisher()
                .apply(singletonMap(record, EXISTING_VALUE))
                .findFirst().get();
        assertThat(result.getCells(), equalTo(getGroupByRecordMergedWithFinalValue(finalValue)));
    }

    private String getFinalValue(Integer accumulator) {
        String finalValue = "finalValue";
        when(finisher.apply(accumulator)).thenReturn(finalValue);
        return finalValue;
    }

    private List<Object> getGroupByRecordMergedWithFinalValue(String finalValue) {
        ArrayList<Object> groupByRecordCells = new ArrayList<>(record.getCells());
        groupByRecordCells.add(finalValue);
        return groupByRecordCells;
    }
}