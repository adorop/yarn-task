package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.YarnApplication;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TwoStagesPhaseConfigurer {
    private final TwoStagesPhaseDefinition definition;

    public void configure(YarnApplication yarnApplication) {
        configureCurrentStage(yarnApplication);
        yarnApplication.commitStage();
        configureNextStage(yarnApplication);
        definition.newSchema()
                .ifPresent(yarnApplication::setCurrentSchema);
    }

    private void configureCurrentStage(YarnApplication yarnApplication) {
        StageDefinition currentStage = yarnApplication.configure();
        currentStage.getTransformerDefinitions().add(definition.transformation());
        currentStage.setWriterDefinition(definition.writer());
    }

    private void configureNextStage(YarnApplication yarnApplication) {
        StageDefinition nextStage = yarnApplication.configure();
        nextStage.setReaderDefinition(definition.nextStageReader());
        nextStage.getTransformerDefinitions().add(definition.nextStageTransformation());
        nextStage.setParallelism(definition.nextStageParallelism());
    }
}
