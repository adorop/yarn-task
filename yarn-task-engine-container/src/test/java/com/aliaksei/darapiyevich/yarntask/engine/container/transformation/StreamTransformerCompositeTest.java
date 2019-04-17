package com.aliaksei.darapiyevich.yarntask.engine.container.transformation;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StreamTransformerCompositeTest {
    private static final Record RECORD = new Record(Collections.emptyList());
    private static final Stream<Record> INPUT = Stream.of(RECORD);

    @Mock
    private StreamTransformer firstTransformer;
    @Mock
    private StreamTransformer secondTransformer;

    private StreamTransformerComposite composite;

    @Before
    public void setUp() throws Exception {
        composite = new StreamTransformerComposite(getComponents());
    }

    private List<StreamTransformer> getComponents() {
        return Arrays.asList(firstTransformer, secondTransformer);
    }

    @Test
    public void shouldReturnStreamTransformedByEachTransformerInOrder() {
        Stream<Record> transformedStream = getTransformedStreamByEach();
        Stream<Record> result = composite.apply(INPUT);
        assertThat(result, equalTo(transformedStream));
    }

    private Stream<Record> getTransformedStreamByEach() {
        Stream<Record> transformedByFirst = getTransformedByFirst();
        return getTransformedBySecond(transformedByFirst);
    }

    private Stream<Record> getTransformedByFirst() {
        Stream<Record> stream = Stream.of(RECORD, RECORD, RECORD);
        when(firstTransformer.apply(INPUT)).thenReturn(stream);
        return stream;
    }

    private Stream<Record> getTransformedBySecond(Stream<Record> transformedByFirst) {
        Stream<Record> stream = Stream.of(RECORD, RECORD, RECORD, RECORD, RECORD);
        when(secondTransformer.apply(transformedByFirst)).thenReturn(stream);
        return stream;
    }
}