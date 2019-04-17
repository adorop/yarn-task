package com.aliaksei.darapiyevich.yarntask.engine.container.writer;

import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.Serializer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;

import static com.aliaksei.darapiyevich.yarntask.engine.container.writer.FileSystemWriter.OVERWRITE;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FileSystemWriterTest {
    private static final byte[] SERIALIZED_ELEMENT = new byte[]{32, 42, 52};

    private final Record input = new Record(Collections.emptyList());

    @Mock
    private Serializer serializer;
    @Mock
    private Path outputPath;
    @Mock
    private FileSystem fileSystem;
    @Mock
    private FSDataOutputStream outputStream;

    private FileSystemWriter writer;

    @Before
    public void setUp() throws Exception {
        initOutputStream();
        initSerializer();
        writer = new FileSystemWriter(fileSystem, outputPath, serializer);
        writer.init();
    }

    private void initOutputStream() throws IOException {
        when(fileSystem.create(outputPath, OVERWRITE)).thenReturn(outputStream);
    }

    private void initSerializer() {
        when(serializer.serialize(input)).thenReturn(SerDeResult.successful(SERIALIZED_ELEMENT));
    }

    @Test
    public void shouldWriteSerializedElementToOutputStream() throws IOException {
        writer.write(input);
        verify(outputStream).write(SERIALIZED_ELEMENT);
    }

    @Test
    public void closeShouldFlushAndCloseOutputStream() throws Exception {
        writer.close();
        verify(outputStream).flush();
        verify(outputStream).close();
    }
}