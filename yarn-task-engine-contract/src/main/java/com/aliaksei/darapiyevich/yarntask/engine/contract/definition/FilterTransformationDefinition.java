package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.predicate.Predicate;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class FilterTransformationDefinition extends TransformerDefinition {
    private Predicate predicate;
}
