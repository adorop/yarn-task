package com.aliaksei.darapiyevich.yarntask.engine.container.transformation;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.predicate.Predicate;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.PrimitiveType;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FilterStreamTransformerTest {
    private static final String FILTER_COLUMN = "secondColumn";

    private static final Schema SCHEMA = Schema.builder()
            .field(new Field("firstColumn", PrimitiveType.DOUBLE))
            .field(new Field(FILTER_COLUMN, PrimitiveType.INTEGER))
            .field(new Field("thirdColumn", PrimitiveType.BOOLEAN))
            .build();

    private Stream<Record> input = Stream.of(
            new Record(Arrays.asList(0.1, 0, true)),
            new Record(Arrays.asList(0.2, 1, true))
    );

    @Mock
    private BiPredicate<Object, Object> predicateFunction;

    private FilterStreamTransformer transformer;

    @Before
    public void setUp() throws Exception {
        transformer = new FilterStreamTransformer(SCHEMA, getPredicate());
    }

    private Predicate getPredicate() {
        Predicate predicate = new Predicate() {
            @Override
            public BiPredicate<Object, Object> getFunction() {
                return predicateFunction;
            }
        };
        predicate.setColumn(FILTER_COLUMN);
        predicate.setValue(0);
        return predicate;
    }

    @Test
    public void shouldNotFilterOutRecordsWhenTheySatisfyPredicate() {
        givenRecordsSatisfyPredicate();
        Stream<Record> result = transformer.apply(input);
        assertThat(result.count(), is(2L));
    }

    private void givenRecordsSatisfyPredicate() {
        when(predicateFunction.test(any(), any())).thenReturn(true);
    }


    @Test
    public void shouldFilterOutElementsThatDoNotSatisfyPredicate() {
        oneRecordFromGivenSatisfiesPredicate();
        Stream<Record> result = transformer.apply(input);
        assertThat(result.count(), is(1L));
    }

    private void oneRecordFromGivenSatisfiesPredicate() {
        when(predicateFunction.test(0, 0)).thenReturn(true);
    }
}