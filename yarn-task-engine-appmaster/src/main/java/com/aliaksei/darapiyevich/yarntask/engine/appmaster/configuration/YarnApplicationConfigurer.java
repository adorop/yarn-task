package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageDefinitionReaderFactory;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManagerFactory;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class YarnApplicationConfigurer {
    private final WarehousePathsManagerFactory warehousePathsManagerFactory;
    private final StageDefinitionReaderFactory stageDefinitionReaderFactory;

    public ReaderConfiguration read(String format) {
        return new ReaderConfiguration(format, new YarnApplication(warehousePathsManagerFactory, stageDefinitionReaderFactory));
    }
}
