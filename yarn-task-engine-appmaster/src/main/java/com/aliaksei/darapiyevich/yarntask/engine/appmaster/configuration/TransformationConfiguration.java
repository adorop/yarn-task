package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.AggregatePhaseDefinitionFactory;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.TopNPhaseDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.TwoStagesPhaseConfigurer;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.TwoStagesPhaseDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.FilterTransformationDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.predicate.Predicate;

public class TransformationConfiguration {
    private final YarnApplication yarnApplication;
    private final AggregatePhaseDefinitionFactory aggregatePhaseDefinitionFactory;

    public TransformationConfiguration(YarnApplication yarnApplication) {
        this.yarnApplication = yarnApplication;
        aggregatePhaseDefinitionFactory = new AggregatePhaseDefinitionFactory(yarnApplication);
    }

    public TransformationConfiguration where(Predicate predicate) {
        FilterTransformationDefinition filterTransformationDefinition = new FilterTransformationDefinition();
        filterTransformationDefinition.setPredicate(predicate);
        filterTransformationDefinition.setSchema(yarnApplication.getCurrentSchema());
        yarnApplication.configure().getTransformerDefinitions()
                .add(filterTransformationDefinition);
        return this;
    }

    public TransformationConfiguration aggregate(Aggregation aggregation, int parallelism) {
        TwoStagesPhaseDefinition aggregationPhaseDefinitions = aggregatePhaseDefinitionFactory.create(aggregation, parallelism);
        new TwoStagesPhaseConfigurer(aggregationPhaseDefinitions).configure(yarnApplication);
        return this;
    }


    public TransformationConfiguration top(int count, String column) {
        TopNPhaseDefinition topNPhaseDefinitions = getTopNPhaseDefinitions(count, column);
        new TwoStagesPhaseConfigurer(topNPhaseDefinitions).configure(yarnApplication);
        return this;
    }

    private TopNPhaseDefinition getTopNPhaseDefinitions(int count, String column) {
        return new TopNPhaseDefinition(
                yarnApplication.getCurrentSchema(),
                count,
                column,
                yarnApplication.getPathsManager(),
                yarnApplication.configure().getParallelism()
        );
    }

    public WriterConfiguration to(String directory) {
        return new WriterConfiguration(yarnApplication, directory);
    }
}
