package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.AggregationTransformationDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TransformerDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class CountAggregationPhaseDefinition implements TwoStagesPhaseDefinition {
    private static final String INTER_STAGES_FORMAT = "csv";

    private final Aggregation<?, ?> aggregation;
    private final Schema initialSchema;
    private final WarehousePathsManager pathsManager;
    private final int currentStageParallelism;
    private final int nextStageParallelism;

    @Override
    public TransformerDefinition transformation() {
        AggregationTransformationDefinition transformationDefinition = new AggregationTransformationDefinition();
        transformationDefinition.setAggregationType(aggregation.getType());
        transformationDefinition.setAlias(aggregation.getAlias());
        transformationDefinition.setGroupByKeys(aggregation.getColumns());
        transformationDefinition.setSchema(initialSchema);
        return transformationDefinition;
    }

    @Override
    public StageWriterDefinition writer() {
        StageWriterDefinition writerDefinition = new StageWriterDefinition();
        StageWriterPartitionsDefinition partitionsDefinition = new StageWriterPartitionsDefinition();
        partitionsDefinition.setSchema(newSchema().get());
        partitionsDefinition.setPartitionByKeys(aggregation.getColumns());
        writerDefinition.setPartitionsDefinition(partitionsDefinition);
        writerDefinition.setFormat(INTER_STAGES_FORMAT);
        writerDefinition.setPaths(getPaths());
        return writerDefinition;
    }

    private List<String> getPaths() {
        return pathsManager.getInterStagesPaths(currentStageParallelism, nextStageParallelism);
    }

    @Override
    public ReaderDefinition nextStageReader() {
        ReaderDefinition readerDefinition = new ReaderDefinition();
        readerDefinition.setDataSchema(newSchema().get());
        readerDefinition.setFormat(INTER_STAGES_FORMAT);
        readerDefinition.setPaths(getPaths());
        return readerDefinition;
    }

    @Override
    public TransformerDefinition nextStageTransformation() {
        AggregationTransformationDefinition transformationDefinition = new AggregationTransformationDefinition();
        transformationDefinition.setSchema(newSchema().get());
        transformationDefinition.setGroupByKeys(aggregation.getColumns());
        transformationDefinition.setAlias(aggregation.getAlias());
        transformationDefinition.setAggregateOperandColumn(aggregation.getAlias());
        transformationDefinition.setAggregationType("sum");
        return transformationDefinition;
    }

    @Override
    public int nextStageParallelism() {
        return nextStageParallelism;
    }

    @Override
    public Optional<Schema> newSchema() {
        Schema newSchema = Schema.builder()
                .fields(getGroupByFields())
                .field(getNewAggregationField())
                .build();
        return Optional.of(newSchema);
    }

    private List<Field> getGroupByFields() {
        return initialSchema.getFields().stream()
                .filter(field -> aggregation.getColumns().contains(field.getName()))
                .collect(toList());
    }

    private Field getNewAggregationField() {
        return new Field(aggregation.getAlias(), aggregation.getNewFieldType());
    }
}
