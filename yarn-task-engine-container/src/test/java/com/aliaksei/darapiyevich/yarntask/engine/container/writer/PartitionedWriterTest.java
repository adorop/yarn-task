package com.aliaksei.darapiyevich.yarntask.engine.container.writer;

import com.aliaksei.darapiyevich.yarntask.engine.container.Writer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedWriterTest {
    private static final Record ELEMENT = new Record(Collections.emptyList());

    @Mock
    private Partitioner partitioner;
    @Mock
    private Writer zeroPartitionWriter;
    @Mock
    private Writer firstPartitionWriter;
    private List<Writer> partitionWriters;

    private PartitionedWriter partitionedWriter;

    @Before
    public void setUp() throws Exception {
        initPartitionWriters();
        partitionedWriter = new PartitionedWriter(partitioner, partitionWriters);
    }

    private void initPartitionWriters() {
        partitionWriters = Arrays.asList(zeroPartitionWriter, firstPartitionWriter);
    }

    @Test
    public void initShouldInitAllParitionWriters() {
        partitionedWriter.init();
        partitionWriters.forEach(writer -> verify(writer).init());
    }

    @Test
    public void shouldWriteElementToPartitionUnderIndexGivenByPartitioner() {
        int givenPartition = getGivenByPartitionerIndex();
        partitionedWriter.write(ELEMENT);
        verify(partitionWriters.get(givenPartition)).write(ELEMENT);
    }

    private int getGivenByPartitionerIndex() {
        int partition = 1;
        when(partitioner.getPartition(ELEMENT)).thenReturn(partition);
        return partition;
    }

    @Test
    public void closeShouldCloseAllPartitionWriters() throws Exception {
        partitionedWriter.close();
        partitionWriters.forEach(writer -> verifyCloses(writer));
    }

    @SneakyThrows
    private void verifyCloses(Writer writer) {
        verify(writer).close();
    }
}