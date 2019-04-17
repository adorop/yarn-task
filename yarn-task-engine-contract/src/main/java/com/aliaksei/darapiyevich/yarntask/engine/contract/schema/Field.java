package com.aliaksei.darapiyevich.yarntask.engine.contract.schema;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;

import java.util.Comparator;
import java.util.function.Function;

import static lombok.AccessLevel.NONE;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(of = "name")
public class Field {
    private String name;
    @Getter(NONE)
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private Type type;

    public Function<String, Object> getFromStringParser() {
        return type.getFromStringParser();
    }

    public Comparator<Object> getComparator() {
        return type.getComparator();
    }
}
