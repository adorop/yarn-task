package com.aliaksei.darapiyevich.yarntask.engine.container.configuration.factory;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.container.transformation.FilterStreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.container.transformation.StreamTransformerComposite;
import com.aliaksei.darapiyevich.yarntask.engine.container.transformation.TopNStreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.container.transformation.aggregate.AggregateStreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregations;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.*;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class StreamTransformerFactory {

    public StreamTransformer create(TaskDefinition taskDefinition) {
        return new StreamTransformerComposite(getTransformerComponents(taskDefinition));
    }

    private List<StreamTransformer> getTransformerComponents(TaskDefinition taskDefinition) {
        return taskDefinition.getTransformerDefinitions().stream()
                .map(this::getTransformer)
                .collect(toList());
    }

    private StreamTransformer getTransformer(TransformerDefinition transformerDefinition) {
        if (transformerDefinition.getClass() == FilterTransformationDefinition.class) {
            return createFilterStreamTransformer(((FilterTransformationDefinition) transformerDefinition));
        } else if (transformerDefinition.getClass() == AggregationTransformationDefinition.class) {
            return createAggregateStreamTransformer(((AggregationTransformationDefinition) transformerDefinition));
        } else {
            return createTopNStreamTransformer(((TopNTransformationDefinition) transformerDefinition));
        }
    }

    private TopNStreamTransformer createTopNStreamTransformer(TopNTransformationDefinition transformerDefinition) {
        return new TopNStreamTransformer(transformerDefinition.getLimit(), transformerDefinition.getSchema(), transformerDefinition.getSortColumn());
    }

    private StreamTransformer createAggregateStreamTransformer(AggregationTransformationDefinition transformationDefinition) {
        List<String> groupByKeys = transformationDefinition.getGroupByKeys();
        Aggregation<?, ?> aggregation = Aggregations.of(transformationDefinition.getAggregationType(), transformationDefinition.getAggregateOperandColumn())
                .by(groupByKeys)
                .as(transformationDefinition.getAlias());
        return new AggregateStreamTransformer<>(aggregation,
                transformationDefinition.getSchema());
    }


    private FilterStreamTransformer createFilterStreamTransformer(FilterTransformationDefinition transformerDefinition) {
        return new FilterStreamTransformer(transformerDefinition.getSchema(), transformerDefinition.getPredicate());
    }
}
