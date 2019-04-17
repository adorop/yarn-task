package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TopNTransformationDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TransformerDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class TopNPhaseDefinition implements TwoStagesPhaseDefinition {
    private static final String INTER_STAGES_FORMAT = "csv";
    private static final int NEXT_STAGE_PARALLELISM = 1;

    private final Schema schema;
    private final int limit;
    private final String column;
    private final WarehousePathsManager pathsManager;
    private final int currentStageParallelism;

    @Override
    public TopNTransformationDefinition transformation() {
        TopNTransformationDefinition definition = new TopNTransformationDefinition();
        definition.setSchema(schema);
        definition.setLimit(limit);
        definition.setSortColumn(column);
        return definition;
    }

    @Override
    public StageWriterDefinition writer() {
        StageWriterDefinition writerDefinition = new StageWriterDefinition();
        writerDefinition.setFormat(INTER_STAGES_FORMAT);
        writerDefinition.setPaths(getPaths());
        return writerDefinition;
    }

    private List<String> getPaths() {
        return pathsManager.getInterStagesPaths(currentStageParallelism, 1);
    }

    @Override
    public ReaderDefinition nextStageReader() {
        ReaderDefinition readerDefinition = new ReaderDefinition();
        readerDefinition.setPaths(getPaths());
        readerDefinition.setDataSchema(schema);
        readerDefinition.setFormat(INTER_STAGES_FORMAT);
        return readerDefinition;
    }

    @Override
    public TransformerDefinition nextStageTransformation() {
        return transformation();
    }

    @Override
    public int nextStageParallelism() {
        return NEXT_STAGE_PARALLELISM;
    }

    @Override
    public Optional<Schema> newSchema() {
        return Optional.empty();
    }
}
