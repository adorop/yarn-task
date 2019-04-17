package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.Data;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

import static lombok.AccessLevel.NONE;

@Data
public class ReaderDefinition {
    private String format;
    private List<String> paths;
    private Schema dataSchema;
    @Getter(NONE)
    private Schema selectSchema;

    public Optional<Schema> getSelectSchema() {
        return Optional.ofNullable(selectSchema);
    }
}
