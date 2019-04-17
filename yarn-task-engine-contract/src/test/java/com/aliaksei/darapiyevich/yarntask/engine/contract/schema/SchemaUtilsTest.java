package com.aliaksei.darapiyevich.yarntask.engine.contract.schema;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class SchemaUtilsTest {
    private static final String FIRST_COLUMN_TO_SELECT = "columns";
    private static final String SECOND_COLUMN_TO_SELECT = "to";

    @Mock
    private Type type;
    private List<String> columnsToSelect = Arrays.asList(FIRST_COLUMN_TO_SELECT, SECOND_COLUMN_TO_SELECT);
    private Schema schema;
    private List<Field> fieldsToSelect;

    @Before
    public void setUp() throws Exception {
        initFieldsToSelect();
        initSchema();
    }

    private void initFieldsToSelect() {
        fieldsToSelect = Arrays.asList(new Field(FIRST_COLUMN_TO_SELECT, type), new Field(SECOND_COLUMN_TO_SELECT, type));
    }

    private void initSchema() {
        schema = Schema.builder()
                .field(fieldsToSelect.get(0))
                .field(new Field("columnToIgnore", type))
                .field(fieldsToSelect.get(1))
                .build();
    }

    @Test
    public void shouldReturnIndexesOfFieldsOfGivenSchemaWithNamesEqualToGivenColumns() {
        List<Integer> result = SchemaUtils.getCellIndexes(schema, columnsToSelect);
        assertThat(result, equalTo(Arrays.asList(0, 2)));
    }

    @Test
    public void getCellIndexShouldReturnIndexOfFieldWhichMatchesColumnName() {
        Integer result = SchemaUtils.getCellIndex(schema, SECOND_COLUMN_TO_SELECT);
        assertThat(result, is(2));
    }

    @Test
    public void selectFieldsShouldReturnFieldsOfGivenSchemaWithNamesEqualToGivenColumns() {
        List<Field> result = SchemaUtils.selectFields(schema, FIRST_COLUMN_TO_SELECT, SECOND_COLUMN_TO_SELECT);
        assertThat(result, equalTo(fieldsToSelect));
    }
}