package com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv;

import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import org.junit.Test;

import java.util.Arrays;

import static com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv.CsvProperties.DEFAULT_LINE_BREAK;
import static com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv.CsvProperties.DEFAULT_SEPARATOR;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DefaultCsvSerializerTest {
    private static final String FIRST_CELL = "first";
    private static final String SECOND_CELL = "value";
    private static final int THIRD_CELL = 0;

    private static final Record INPUT = new Record(Arrays.asList(FIRST_CELL, SECOND_CELL, THIRD_CELL));

    private static final String EXPECTED_RESULT = FIRST_CELL + DEFAULT_SEPARATOR +
            SECOND_CELL + DEFAULT_SEPARATOR +
            THIRD_CELL + DEFAULT_LINE_BREAK;

    private final DefaultCsvSerializer serializer = new DefaultCsvSerializer();

    @Test
    public void shouldReturnRecordsCellsSeparatedByDefaultSeparator() {
        SerDeResult<byte[]> result = serializer.serialize(INPUT);
        assertTrue(result.isSuccessful());
        assertIsCellsSeparatedByDefaultSeparator(result.getResult());
    }

    private void assertIsCellsSeparatedByDefaultSeparator(byte[] result) {
        assertThat(new String(result), equalTo(EXPECTED_RESULT));
    }
}