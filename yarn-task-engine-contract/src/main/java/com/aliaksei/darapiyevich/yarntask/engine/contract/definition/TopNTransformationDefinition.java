package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class TopNTransformationDefinition extends TransformerDefinition {
    private int limit;
    private String sortColumn;
}
