package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.predicate.Predicate;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.PrimitiveType;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class TaskDefinitionSerializerTest {
    private static final List<String> GROUP_BY_KEYS = Arrays.asList("group", "by", "keys");
    private static final String PATH = "some/path";

    private final Schema schema = Schema.builder()
            .field(new Field("name", PrimitiveType.INTEGER))
            .build();

    private final TaskDefinitionSerializer serializer = new TaskDefinitionSerializer();

    @Test
    public void testSerDe() {
        TaskDefinition initial = createStageDefinition();
        byte[] serialized = serializer.serialize(initial);
        TaskDefinition deserialized = serializer.deserialize(new ByteArrayInputStream(serialized));
        assertThat(deserialized, equalTo(initial));
    }

    private TaskDefinition createStageDefinition() {
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setReaderDefinition(createReaderDefinition());
        taskDefinition.setTransformerDefinitions(createTransformerDefinitions());
        taskDefinition.setWriterDefinition(createWriterDefinition());
        return taskDefinition;
    }

    private ReaderDefinition createReaderDefinition() {
        ReaderDefinition readerDefinition = new ReaderDefinition();
        readerDefinition.setDataSchema(schema);
        readerDefinition.setSelectSchema(schema);
        readerDefinition.setPaths(Collections.singletonList(PATH));
        return readerDefinition;
    }

    private List<TransformerDefinition> createTransformerDefinitions() {
        return Arrays.asList(
                createAggregationDefinition(),
                createTopNTransformationDefinition(),
                createFilterTransformationDefinition());
    }

    private AggregationTransformationDefinition createAggregationDefinition() {
        AggregationTransformationDefinition aggregationDefinition = new AggregationTransformationDefinition();
        aggregationDefinition.setAggregationType("type");
        aggregationDefinition.setSchema(schema);
        aggregationDefinition.setAlias("alias");
        aggregationDefinition.setGroupByKeys(GROUP_BY_KEYS);
        return aggregationDefinition;
    }

    private TopNTransformationDefinition createTopNTransformationDefinition() {
        TopNTransformationDefinition topNTransformationDefinition = new TopNTransformationDefinition();
        topNTransformationDefinition.setLimit(2);
        topNTransformationDefinition.setSchema(schema);
        topNTransformationDefinition.setSortColumn("sortColumn");
        return topNTransformationDefinition;
    }

    private FilterTransformationDefinition createFilterTransformationDefinition() {
        FilterTransformationDefinition filterTransformationDefinition = new FilterTransformationDefinition();
        filterTransformationDefinition.setSchema(schema);
        filterTransformationDefinition.setPredicate(createPredicate());
        return filterTransformationDefinition;
    }

    private Predicate createPredicate() {
        return Predicate.PredicateBuilder.column("column")
                .eq("value");
    }

    private WriterDefinition createWriterDefinition() {
        WriterDefinition writerDefinition = new WriterDefinition();
        writerDefinition.setSinglePath(PATH);
        writerDefinition.setPartitionsDefinition(createPartitionsDefinition());
        return writerDefinition;
    }

    private PartitionsDefinition createPartitionsDefinition() {
        PartitionsDefinition partitionsDefinition = new PartitionsDefinition();
        partitionsDefinition.setPartitionByKeys(GROUP_BY_KEYS);
        partitionsDefinition.setPaths(Collections.singletonList(PATH));
        partitionsDefinition.setSchema(schema);
        return partitionsDefinition;
    }
}