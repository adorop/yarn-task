package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class AddOptionToRunContainerCommandTest {
    private static final String OPTION = "option";
    private static final List<String> EXPECTED = Arrays.asList("java", "-jar", "jarname.jar", OPTION, "1>outLocation", "2>errLocation");

    private List<String> runContainerCommand = Arrays.asList("java", "-jar", "jarname.jar", "1>outLocation", "2>errLocation");

    private AddOptionToRunContainerCommand addOptionToRunContainerCommand = new AddOptionToRunContainerCommand();

    @Test
    public void shouldInsertOptionBefore_OUT_redirection() {
        List<String> result = addOptionToRunContainerCommand.apply(OPTION, runContainerCommand);
        assertThat(result, equalTo(EXPECTED));
    }
}