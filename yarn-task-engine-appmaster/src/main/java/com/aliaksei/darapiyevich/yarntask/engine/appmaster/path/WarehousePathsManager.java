package com.aliaksei.darapiyevich.yarntask.engine.appmaster.path;

import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class WarehousePathsManager {
    static final String WAREHOUSE_DIRECTORY = "/_yarn_app_warehouse/";

    static final String INTER_STAGES_RESULTS_DIRECTORY = "intermediate_results/";
    static final String DEFINITIONS_DIRECTORY = "definitions/";

    private final String applicationName;
    private final int stageIndex;

    public List<String> getInterStagesPaths(int currentStageParallelism, int nextStageParallelism) {
        return IntStream.range(0, currentStageParallelism)
                .mapToObj(containerIndex -> perContainerPaths(containerIndex, nextStageParallelism))
                .flatMap(Collection::stream)
                .collect(toList());
    }

    private List<String> perContainerPaths(int containerIndex, int nextStageParallelism) {
        return IntStream.range(0, nextStageParallelism)
                .mapToObj(i -> WAREHOUSE_DIRECTORY +
                        applicationName + "/" +
                        INTER_STAGES_RESULTS_DIRECTORY +
                        "stage_" + stageIndex + "/" +
                        "part_" + containerIndex + "_" + i
                )
                .collect(toList());
    }

    public String getDefinitionPath(int containerIndex) {
        return WAREHOUSE_DIRECTORY +
                applicationName + "/" +
                DEFINITIONS_DIRECTORY +
                stageIndex + "_" + containerIndex;
    }

    public static String getApplicationWarehouseDirectory(String applicationName) {
        return WAREHOUSE_DIRECTORY + applicationName + "/";
    }
}
