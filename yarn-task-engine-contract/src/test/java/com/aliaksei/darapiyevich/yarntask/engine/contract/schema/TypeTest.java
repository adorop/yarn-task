package com.aliaksei.darapiyevich.yarntask.engine.contract.schema;

import org.junit.Test;

import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TypeTest {

    private final Function<String, Object> booleanParser = PrimitiveType.BOOLEAN.getFromStringParser();

    @Test
    public void booleanParserShouldReturnTrueWhenValueIs_1_Or_true() {
        assertThat(booleanParser.apply("1"), is(true));
        assertThat(booleanParser.apply("true"), is(true));
    }

    @Test
    public void booleanParserShouldReturnFalseWhenValueIs_0_Or_false() {
        assertThat(booleanParser.apply("0"), is(false));
        assertThat(booleanParser.apply("false"), is(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void booleanParserShouldThrowExceptionWhenValueIsNeitherOfExpectedBooleanValues() {
        booleanParser.apply("not valid");
    }
}