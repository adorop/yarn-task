package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage;

import org.apache.commons.collections.ListUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

public class AddOptionToRunContainerCommand implements BiFunction<String, List<String>, List<String>> {
    private static final String OUT_REDIRECTION_PREFIX = "1>";

    @Override
    public List<String> apply(String option, List<String> runContainerCommands) {
        int outRedirectionIndex = getOutRedirectionIndex(runContainerCommands);
        return concat(
                runContainerCommands.subList(0, outRedirectionIndex),
                option,
                runContainerCommands.subList(outRedirectionIndex, runContainerCommands.size())
        );
    }

    private int getOutRedirectionIndex(List<String> runContainerCommands) {
        return IntStream.range(0, runContainerCommands.size())
                .filter(i -> runContainerCommands.get(i).startsWith(OUT_REDIRECTION_PREFIX))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }

    @SuppressWarnings("unchecked")
    private List<String> concat(List<String> runJar, String option, List<String> redirections) {
        return ListUtils.union(
                ListUtils.union(runJar, Collections.singletonList(option)),
                redirections
        );
    }
}
