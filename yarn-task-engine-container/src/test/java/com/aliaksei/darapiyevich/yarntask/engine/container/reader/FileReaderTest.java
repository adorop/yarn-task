package com.aliaksei.darapiyevich.yarntask.engine.container.reader;

import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.Deserializer;
import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FileReaderTest {
    private final List<String> inputData = Arrays.asList("uncompressed", "input", "data");
    private final List<String> gzippedInputData = Arrays.asList("gzipped", "file");

    private Record deserializedRecordOfNotCompressedInput = new Record(Collections.singletonList("not compressed"));
    private Record deserializedRecordOfGzippedInput = new Record(Collections.singletonList("compressed"));

    @Mock
    private FileSystem fileSystem;
    @Mock
    private Path file;
    private Deserializer deserializer;

    private FileReader reader;

    @Before
    public void setUp() throws Exception {
        initFileName();
        initDeserializer();
        reader = new FileReader(fileSystem, file, deserializer) {
            @Override
            InputStream openDataStream() throws IOException {
                return lines(inputData);
            }

            private ByteArrayInputStream lines(List<String> inputData) {
                return new ByteArrayInputStream(inputData.stream().collect(joining("\n")).getBytes());
            }

            @Override
            InputStream wrapInGzipped(InputStream inputStream) throws IOException {
                return lines(gzippedInputData);
            }
        };
    }

    private void initFileName() {
        when(file.getName()).thenReturn("fileName");
    }

    private void initDeserializer() {
        deserializer = s -> inputData.contains(s) ?
                SerDeResult.successful(deserializedRecordOfNotCompressedInput) :
                SerDeResult.successful(deserializedRecordOfGzippedInput);
    }


    @Test
    public void shouldReturnDeserializedElementsOfInputStream() {
        Stream<Record> result = reader.get();
        assertIsDeserializedElementsOfInputStream(result);
    }

    private void assertIsDeserializedElementsOfInputStream(Stream<Record> result) {
        result.forEach(record -> assertThat(record, equalTo(deserializedRecordOfNotCompressedInput)));
    }

    @Test
    public void shouldReturnUngzippedDeserializedElementsOfInputStreamWhenFileHas_GZ_extension() {
        givenFileHas_GZ_extension();
        Stream<Record> result = reader.get();
        assertIsUngzippredDeserializedElements(result);
    }

    private void assertIsUngzippredDeserializedElements(Stream<Record> result) {
        result.forEach(record -> assertThat(record, equalTo(deserializedRecordOfGzippedInput)));
    }

    private void givenFileHas_GZ_extension() {
        when(file.getName()).thenReturn("filename.gz");
    }
}