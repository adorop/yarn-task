package com.aliaksei.darapiyevich.yarntask.engine.container.serialization.csv;

import com.aliaksei.darapiyevich.yarntask.engine.container.serialization.SerDeResult;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCsvDeserializerTest {
    private static final Object PARSED_VALUE = new Object();
    private static final String INPUT = "0,true";

    @Mock
    private Function<String, Object> fromStringParser;
    @Mock
    private Type type;

    private DefaultCsvDeserializer deserializer;

    @Before
    public void setUp() throws Exception {
        initParser();
        deserializer = new DefaultCsvDeserializer(getSchema());
    }

    private void initParser() {
        when(type.getFromStringParser()).thenReturn(fromStringParser);
        when(fromStringParser.apply(anyString())).thenReturn(PARSED_VALUE);
    }

    private Schema getSchema() {
        return Schema.builder()
                .field(new Field("firstColumn", type))
                .field(new Field("secondColumn", type))
                .build();
    }

    @Test
    public void shouldReturnSuccessfulResultWithParsedCells() {
        SerDeResult<Record> result = deserializer.apply(INPUT);
        assertIsSuccessfulWithParsedCells(result);
    }


    private void assertIsSuccessfulWithParsedCells(SerDeResult<Record> result) {
        assertTrue(result.isSuccessful());
        List<Object> cells = result.getResult().getCells();
        assertThat(cells, Matchers.hasSize(getSchema().getFields().size()));
        cells.forEach(cell -> assertThat(cell, equalTo(PARSED_VALUE)));
    }

    @Test
    public void shouldReturnFailedResultWhenParserCannotParseValue() {
        parserCannotParseSecondValueFromGiven();
        SerDeResult result = deserializer.apply(INPUT);
        assertFalse(result.isSuccessful());
    }

    private void parserCannotParseSecondValueFromGiven() {
        when(fromStringParser.apply("true")).thenThrow(new RuntimeException());
    }

    @Test
    public void shouldNotParseValueWhichIsNotIncludedInSelectSchema() {
        DefaultCsvDeserializer deserializer = new DefaultCsvDeserializer(getSchema(), getSelectSchemaWithoutSecondField());
        deserializer.apply(INPUT);
        verifyDoesNotParseSecondValue();
    }

    private Schema getSelectSchemaWithoutSecondField() {
        return Schema.builder()
                .field(getSchema().getFields().get(0))
                .build();
    }

    private void verifyDoesNotParseSecondValue() {
        verify(fromStringParser, never()).apply("true");
    }
}