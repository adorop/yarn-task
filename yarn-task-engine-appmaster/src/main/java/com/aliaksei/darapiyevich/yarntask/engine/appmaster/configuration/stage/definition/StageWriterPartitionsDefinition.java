package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.Data;

import java.util.List;

@Data
public class StageWriterPartitionsDefinition {
    private Schema schema;
    private List<String> partitionByKeys;
}
