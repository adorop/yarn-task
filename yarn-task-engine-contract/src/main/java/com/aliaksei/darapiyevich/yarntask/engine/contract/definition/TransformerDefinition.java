package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class TransformerDefinition {
    private Schema schema;
}
