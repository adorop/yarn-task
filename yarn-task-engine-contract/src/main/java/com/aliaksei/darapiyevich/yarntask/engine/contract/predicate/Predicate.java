package com.aliaksei.darapiyevich.yarntask.engine.contract.predicate;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.function.BiPredicate;

import static lombok.AccessLevel.NONE;

@Data
public class Predicate {
    private String column;
    @Getter(value = NONE)
    @Setter(value = NONE)
    private Operator operator;
    private Object value;

    public BiPredicate<Object, Object> getFunction() {
        return operator.getFunction();
    }

    public static class PredicateBuilder {
        private String column;

        public static PredicateBuilder column(String name) {
            PredicateBuilder builder = new PredicateBuilder();
            builder.column = name;
            return builder;
        }

        public Predicate eq(Object value) {
            Predicate predicate = new Predicate();
            predicate.column = this.column;
            predicate.operator = Operator.EQ;
            predicate.value = value;
            return predicate;
        }
    }
}
