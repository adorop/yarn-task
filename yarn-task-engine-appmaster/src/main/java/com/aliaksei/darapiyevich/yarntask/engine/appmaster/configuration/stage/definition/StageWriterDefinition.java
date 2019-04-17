package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import lombok.Data;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

import static lombok.AccessLevel.NONE;

@Data
public class StageWriterDefinition {
    private List<String> paths;
    @Getter(NONE)
    private StageWriterPartitionsDefinition partitionsDefinition;
    private String format;

    public Optional<StageWriterPartitionsDefinition> getPartitionsDefinition() {
        return Optional.ofNullable(partitionsDefinition);
    }
}
