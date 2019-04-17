package com.aliaksei.darapiyevich.yarntask.engine.appmaster.path;

import java.util.List;

public class StagePathsDistributor {
    public List<String> getPerTaskPaths(List<String> perStagePaths, int parallelism, int taskIndex) {
        int perTask = perStagePaths.size() / parallelism;
        int fromIndex = perTask * taskIndex;
        return perStagePaths.subList(fromIndex, fromIndex + perTask);
    }
}
