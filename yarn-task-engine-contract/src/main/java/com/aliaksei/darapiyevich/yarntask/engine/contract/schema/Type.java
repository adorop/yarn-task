package com.aliaksei.darapiyevich.yarntask.engine.contract.schema;

import java.util.Comparator;
import java.util.function.Function;

public interface Type {
    Function<String, Object> getFromStringParser();
    Comparator<Object> getComparator();
}
