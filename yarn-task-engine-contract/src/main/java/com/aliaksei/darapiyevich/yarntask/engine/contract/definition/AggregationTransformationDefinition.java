package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class AggregationTransformationDefinition extends TransformerDefinition {
    private String aggregationType;
    private List<String> groupByKeys;
    private String aggregateOperandColumn;
    private String alias;
}

