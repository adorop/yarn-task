package com.aliaksei.darapiyevich.yarntask.engine.contract.schema;

import lombok.*;

import java.util.List;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Schema {
    @Singular
    private List<Field> fields;
}
