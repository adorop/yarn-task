package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TransformerDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.WriterDefinition;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class StageDefinition {
    private final int id;
    private ReaderDefinition readerDefinition;
    private List<TransformerDefinition> transformerDefinitions = new ArrayList<>();
    private StageWriterDefinition writerDefinition;
    private int parallelism;
}
