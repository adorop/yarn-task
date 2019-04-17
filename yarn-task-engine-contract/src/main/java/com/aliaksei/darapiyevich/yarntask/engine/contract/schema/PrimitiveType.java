package com.aliaksei.darapiyevich.yarntask.engine.contract.schema;

import lombok.Getter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

@Getter
public enum PrimitiveType implements Type {

    DATE_TIME(PrimitiveType::dateTimeParser, Comparator.comparing(dt0 -> ((LocalDateTime) dt0))),
    DATE(PrimitiveType::dateParser, Comparator.comparing(d0 -> ((LocalDate) d0))),
    INTEGER(Integer::valueOf, Comparator.comparing(i0 -> ((Integer) i0))),
    DOUBLE(Double::valueOf, Comparator.comparing(d0 -> ((Double) d0))),
    LONG(Long::valueOf, Comparator.comparing(l0 -> ((Long) l0))),
    BOOLEAN(PrimitiveType::booleanParser, Comparator.comparing(b0 -> ((Boolean) b0)));

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

    private static LocalDateTime dateTimeParser(String s) {
        return LocalDateTime.parse(s, DATE_TIME_FORMATTER);
    }

    private static LocalDate dateParser(String s) {
        return LocalDate.parse(s, DateTimeFormatter.ISO_DATE);
    }

    private final Function<String, Object> fromStringParser;
    private final Comparator<Object> comparator;

    private static final List<String> BOOLEAN_TRUE_VALUES = Arrays.asList("1", "true");
    private static final List<String> BOOLEAN_FALSE_VALUES = Arrays.asList("0", "false");

    private static boolean booleanParser(String s) {
        if (BOOLEAN_TRUE_VALUES.contains(s)) {
            return true;
        }
        if (BOOLEAN_FALSE_VALUES.contains(s)) {
            return false;
        }
        throw new IllegalArgumentException(String.format("Cannot parse %s to boolean", s));
    }

    PrimitiveType(Function<String, Object> fromString, Comparator<Object> comparator) {
        this.fromStringParser = fromString;
        this.comparator = comparator;
    }
}
