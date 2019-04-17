package com.aliaksei.darapiyevich.yarntask.engine.container.transformation.aggregate;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregations;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class AggregateStreamTransformerTest {
    private static final String FIRST_GROUP_BY_COLUMN = "firstGroupByColumn";
    private static final String SECOND_GROUP_BY_COLUMN = "secondGroupByColumn";
    private static final String SUM_COLUMN = "sumColumn";

    private static final Pair<Integer, String> KEY_TO_APPEAR_TWICE = new Pair<>(1, "twice");
    private static final Pair<Integer, String> KEY_TO_APPEAR_ONCE = new Pair<>(5, "once");

    private final Stream<Record> input = Stream.of(
            new Record(Arrays.asList(KEY_TO_APPEAR_TWICE.getFirst(), 8L, KEY_TO_APPEAR_TWICE.getSecond())),
            new Record(Arrays.asList(KEY_TO_APPEAR_ONCE.getFirst(), 3L, KEY_TO_APPEAR_ONCE.getSecond())),
            new Record(Arrays.asList(KEY_TO_APPEAR_TWICE.getFirst(), 2L, KEY_TO_APPEAR_TWICE.getSecond()))
    );


    @Mock
    private Type type;

    private Schema getSchema() {
        return Schema.builder()
                .field(new Field(FIRST_GROUP_BY_COLUMN, type))
                .field(new Field(SUM_COLUMN, type))
                .field(new Field(SECOND_GROUP_BY_COLUMN, type))
                .build();
    }

    @Test
    public void testCountAggregation() {
        List<Record> result = countAggregationTransformer().apply(input)
                .collect(toList());
        assertThat(result, hasItem(expectedRecord(KEY_TO_APPEAR_TWICE, 2L)));
        assertThat(result, hasItem(expectedRecord(KEY_TO_APPEAR_ONCE, 1L)));
    }

    private AggregateStreamTransformer<MutableLong, Long> countAggregationTransformer() {
        return new AggregateStreamTransformer<>(
                Aggregations.count().by(FIRST_GROUP_BY_COLUMN, SECOND_GROUP_BY_COLUMN).as("count"),
                getSchema()
        );
    }

    private Record expectedRecord(Pair<Integer, String> key, long expectedCount) {
        return new Record(Arrays.asList(key.getFirst(), key.getSecond(), expectedCount));
    }

    @Test
    public void testSumAggregation() {
        List<Record> result = sumAggregationTransformer().apply(input)
                .collect(toList());
        assertThat(result, hasItem(expectedRecord(KEY_TO_APPEAR_TWICE, 10L)));
        assertThat(result, hasItem(expectedRecord(KEY_TO_APPEAR_ONCE, 3L)));
    }

    private AggregateStreamTransformer<MutableLong, Number> sumAggregationTransformer() {
        return new AggregateStreamTransformer<>(
                Aggregations.sum(SUM_COLUMN).by(FIRST_GROUP_BY_COLUMN, SECOND_GROUP_BY_COLUMN).as("sum"),
                getSchema()
        );
    }
}