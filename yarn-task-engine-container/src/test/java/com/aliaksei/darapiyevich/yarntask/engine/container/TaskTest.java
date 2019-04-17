package com.aliaksei.darapiyevich.yarntask.engine.container;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TaskTest {

    private final List<Record> firstInputStream = Arrays.asList(
            new Record(Collections.singletonList("element")),
            new Record(Arrays.asList(1, 2, 3))
    );
    private final List<Record> secondInputStream = Collections.singletonList(new Record(Collections.emptyList()));

    private List<Record> elementsOfTransformedStream = Arrays.asList(
            new Record(Arrays.asList(3, 2, 1)),
            new Record(Arrays.asList("transformed", "elements")));

    @Mock
    private StreamReader firstStreamReader;
    @Mock
    private StreamReader secondStreamReader;
    @Mock
    private StreamTransformer streamTransformer;
    @Mock
    private Writer writer;

    private Task task;


    @Before
    public void setUp() throws Exception {
        initInputStream();
        initTransformer();
        task = new Task(Arrays.asList(firstStreamReader, secondStreamReader), streamTransformer, writer);
    }

    private void initInputStream() {
        when(firstStreamReader.get()).thenReturn(firstInputStream.stream());
        when(secondStreamReader.get()).thenReturn(secondInputStream.stream());
    }

    private void initTransformer() {
        when(streamTransformer.apply(any())).thenReturn(elementsOfTransformedStream.stream());
    }

    @Test
    public void executeShouldWriteElementsOfTransformedStreams() {
        task.execute();
        elementsOfTransformedStream.forEach(element -> verify(writer).write(element));
    }

    @Test
    public void executeShouldInitWriterBeforeWriting() {
        task.execute();
        InOrder inOrder = inOrder(writer);
        inOrder.verify(writer).init();
        inOrder.verify(writer, atLeastOnce()).write(any());
    }
}