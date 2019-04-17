package com.aliaksei.darapiyevich.yarntask.engine.appmaster.path;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WarehousePathsManagerFactory {
    private final String applicationName;

    public WarehousePathsManager create(int stageId) {
        return new WarehousePathsManager(applicationName, stageId);
    }
}
