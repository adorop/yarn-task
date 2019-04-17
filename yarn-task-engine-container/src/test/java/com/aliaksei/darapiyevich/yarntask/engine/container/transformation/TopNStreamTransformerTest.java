package com.aliaksei.darapiyevich.yarntask.engine.container.transformation;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopNStreamTransformerTest {
    private static final int LIMIT = 2;
    private static final String BY = "sortByColumn";

    private static final String LARGEST_VALUE = "largest";
    private static final Record RECORD_WITH_LARGEST_VALUE = new Record(Arrays.asList("dummy", LARGEST_VALUE));

    private static final String MEDIUM_VALUE = "medium";
    private static final Record RECORD_WITH_MEDIUM_VALUE = new Record(Arrays.asList("another", MEDIUM_VALUE));

    private static final String SMALLEST_VALUE = "small";
    private static final Record RECORD_WITH_SMALLEST_VALUE = new Record(Arrays.asList("field", SMALLEST_VALUE));

    private final Stream<Record> input = Stream.of(
            RECORD_WITH_MEDIUM_VALUE,
            RECORD_WITH_SMALLEST_VALUE,
            RECORD_WITH_LARGEST_VALUE
    );

    @Mock
    private Type extraType;
    @Mock
    private Type typeOfFieldToCompareBy;
    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private Comparator<Object> comparator;

    private TopNStreamTransformer transformer;

    @Before
    public void setUp() throws Exception {
        initComparator();
        transformer = new TopNStreamTransformer(LIMIT, getSchema(), BY);
    }

    private void initComparator() {
        when(typeOfFieldToCompareBy.getComparator()).thenReturn(comparator);
        when(comparator.compare(eq(LARGEST_VALUE), any())).thenReturn(1);
        when(comparator.compare(any(), eq(LARGEST_VALUE))).thenReturn(-1);
        when(comparator.compare(eq(SMALLEST_VALUE), any())).thenReturn(-1);
        when(comparator.compare(any(), eq(SMALLEST_VALUE))).thenReturn(1);
    }

    private Schema getSchema() {
        return Schema.builder()
                .field(new Field("extraField", extraType))
                .field(new Field(BY, typeOfFieldToCompareBy))
                .build();
    }

    @Test
    public void shouldReturnGivenNumberOfRecordsComparingByComparatorReversedAssociatedWithGivenField() {
        List<Record> result = transformer.apply(input)
                .collect(toList());
        assertIsTop2(result);
    }

    private void assertIsTop2(List<Record> result) {
        assertThat(result, hasSize(2));
        assertThat(result.get(0), equalTo(RECORD_WITH_LARGEST_VALUE));
        assertThat(result.get(1), equalTo(RECORD_WITH_MEDIUM_VALUE));
    }
}