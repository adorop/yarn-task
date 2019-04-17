package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import lombok.Data;
import lombok.Getter;

import java.util.Optional;

import static lombok.AccessLevel.NONE;

@Data
public class WriterDefinition {
    @Getter(NONE)
    private String singlePath;
    @Getter(NONE)
    private PartitionsDefinition partitionsDefinition;
    private String format;

    public Optional<String> getSinglePath() {
        return Optional.ofNullable(singlePath);
    }

    public Optional<PartitionsDefinition> getPartitionsDefinition() {
        return Optional.ofNullable(partitionsDefinition);
    }
}
