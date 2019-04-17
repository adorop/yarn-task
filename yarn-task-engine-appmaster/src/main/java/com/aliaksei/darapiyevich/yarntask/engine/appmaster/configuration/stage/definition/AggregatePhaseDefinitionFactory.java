package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.YarnApplication;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregation;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AggregatePhaseDefinitionFactory {
    private final YarnApplication yarnApplication;

    public TwoStagesPhaseDefinition create(Aggregation<?, ?> aggregation, int nextStageParallelism) {
        if (aggregation.getType().equals("count")) {
            return new CountAggregationPhaseDefinition(
                    aggregation,
                    yarnApplication.getCurrentSchema(),
                    yarnApplication.getPathsManager(),
                    yarnApplication.configure().getParallelism(),
                    nextStageParallelism
            );
        }
        throw new UnsupportedOperationException("'count' is the only supported type");
    }
}
