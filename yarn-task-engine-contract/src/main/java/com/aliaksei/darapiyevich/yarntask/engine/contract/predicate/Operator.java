package com.aliaksei.darapiyevich.yarntask.engine.contract.predicate;

import lombok.Getter;

import java.util.Objects;
import java.util.function.BiPredicate;

public enum Operator {
    EQ(Objects::equals);

    @Getter
    private final BiPredicate<Object, Object> function;

    Operator(BiPredicate<Object, Object> function) {
        this.function = function;
    }
}
