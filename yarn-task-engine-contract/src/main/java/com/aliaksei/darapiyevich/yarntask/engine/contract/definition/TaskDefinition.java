package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import lombok.Data;

import java.util.List;

@Data
public class TaskDefinition {
    private ReaderDefinition readerDefinition;
    private List<TransformerDefinition> transformerDefinitions;
    private WriterDefinition writerDefinition;
}
