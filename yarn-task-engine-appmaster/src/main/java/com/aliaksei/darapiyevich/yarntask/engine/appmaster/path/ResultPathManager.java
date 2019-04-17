package com.aliaksei.darapiyevich.yarntask.engine.appmaster.path;

import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class ResultPathManager {
    private final int parallelism;

    public List<String> getFilesPaths(String directory) {
        return IntStream.range(0, parallelism)
                .mapToObj(toFilePath(directory))
                .collect(toList());
    }

    private IntFunction<String> toFilePath(String directory) {
        return part -> {
            String fileName = directory.endsWith("/") ?
                    "part_" + part :
                    "/part_" + part;
            return directory + fileName;
        };
    }
}
