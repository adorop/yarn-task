package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.AggregationTransformationDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TransformerDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CountAggregationPhaseDefinitionTest {
    private static final int CURRENT_STAGE_PARALLELISM = 1;
    private static final int NEXT_STAGE_PARALLELISM = 2;
    private static final String FIRST_GROUP_BY_KEY = "firstGroupByKey";
    private static final String SECOND_GROUP_BY_KEY = "secondGroupByKey";
    private static final String ALIAS = "alias";

    @Mock
    private Aggregation<AtomicBoolean, Boolean> aggregation;
    @Mock
    private Type newFieldType;
    @Mock
    private WarehousePathsManager pathsManager;
    @Mock
    private Type type;
    private Schema initialSchema;

    private CountAggregationPhaseDefinition definition;

    @Before
    public void setUp() throws Exception {
        initSchema();
        initAggregation();
        definition = new CountAggregationPhaseDefinition(
                aggregation,
                initialSchema,
                pathsManager,
                CURRENT_STAGE_PARALLELISM,
                NEXT_STAGE_PARALLELISM
        );
    }

    private void initSchema() {
        initialSchema = Schema.builder()
                .field(new Field(FIRST_GROUP_BY_KEY, type))
                .field(new Field(SECOND_GROUP_BY_KEY, type))
                .field(new Field("yetOneKey", type))
                .build();
    }

    private void initAggregation() {
        when(aggregation.getColumns()).thenReturn(Arrays.asList(FIRST_GROUP_BY_KEY, SECOND_GROUP_BY_KEY));
        when(aggregation.getAlias()).thenReturn(ALIAS);
        when(aggregation.getNewFieldType()).thenReturn(newFieldType);
    }

    @Test
    public void shouldSetInitialSchemaToTransformation() {
        TransformerDefinition transformation = definition.transformation();
        assertThat(transformation.getSchema(), equalTo(initialSchema));
    }

    @Test
    public void newSchemaShouldReturnSchemaWithFieldsFromInitialSchemaListedInGroupByKeysWithNewAggregationField() {
        Optional<Schema> schema = definition.newSchema();
        assertTrue(schema.isPresent());
        assertThat(schema.get(), equalTo(getExpectedNewSchema()));
    }

    private Schema getExpectedNewSchema() {
        return Schema.builder()
                .field(new Field(FIRST_GROUP_BY_KEY, type))
                .field(new Field(SECOND_GROUP_BY_KEY, type))
                .field(new Field(ALIAS, newFieldType))
                .build();
    }

    @Test
    public void shouldSetNewSchemaToWriter() {
        StageWriterDefinition writer = definition.writer();
        Optional<StageWriterPartitionsDefinition> partitionsDefinition = writer.getPartitionsDefinition();
        assertTrue(partitionsDefinition.isPresent());
        assertThat(partitionsDefinition.get().getSchema(), equalTo(getExpectedNewSchema()));
    }

    @Test
    public void shouldSetNewSchemaToNextStageReaderDataSchema() {
        ReaderDefinition reader = definition.nextStageReader();
        assertThat(reader.getDataSchema(), equalTo(getExpectedNewSchema()));
    }

    @Test
    public void shouldSetFormatOfReaderAndWriterToSameValue() {
        ReaderDefinition reader = definition.nextStageReader();
        StageWriterDefinition writer = definition.writer();
        assertThat(reader.getFormat(), equalTo(writer.getFormat()));
    }

    @Test
    public void shouldSetFormatOfPathsOrReaderAndWriterToSameValues() {
        ReaderDefinition reader = definition.nextStageReader();
        StageWriterDefinition writer = definition.writer();
        assertThat(reader.getPaths(), equalTo(writer.getPaths()));
    }

    @Test
    public void nextStageParallelismShouldBeEqualToGivenOne() {
        int nextStageParallelism = definition.nextStageParallelism();
        assertThat(nextStageParallelism, is(NEXT_STAGE_PARALLELISM));
    }

    @Test
    public void nextStageTransformationShouldBeOf_SUM_typeWithNewSchemaAndAliasAsAggregationOperandColumn() {
        AggregationTransformationDefinition result = ((AggregationTransformationDefinition) definition.nextStageTransformation());
        assertThat(result.getSchema(), equalTo(getExpectedNewSchema()));
        assertThat(result.getAggregateOperandColumn(), equalTo(ALIAS));
        assertThat(result.getAggregationType(), equalTo("sum"));
    }
}