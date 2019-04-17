package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.Data;

import java.util.List;

@Data
public class PartitionsDefinition {
    private List<String> paths;
    private Schema schema;
    private List<String> partitionByKeys;
}
