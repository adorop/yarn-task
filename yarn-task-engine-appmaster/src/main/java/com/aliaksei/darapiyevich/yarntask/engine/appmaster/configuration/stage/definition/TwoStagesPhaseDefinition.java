package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TransformerDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;

import java.util.Optional;

public interface TwoStagesPhaseDefinition {
    TransformerDefinition transformation();
    StageWriterDefinition writer();
    ReaderDefinition nextStageReader();
    TransformerDefinition nextStageTransformation();
    int nextStageParallelism();
    Optional<Schema> newSchema();
}
