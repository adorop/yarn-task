package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.Stage;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageDefinitionReaderFactory;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManagerFactory;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.yarn.am.container.ContainerLauncher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class YarnApplication {
    private final WarehousePathsManagerFactory warehousePathsManagerFactory;
    private final StageDefinitionReaderFactory stageDefinitionReaderFactory;

    private List<StageDefinition> stageDefinitions = new ArrayList<>();
    private int stageInConfiguration = 0;
    @Getter
    @Setter
    private Schema currentSchema;

    public StageDefinition configure() {
        if (stageInConfiguration == stageDefinitions.size()) {
            stageDefinitions.add(new StageDefinition(stageInConfiguration));
        }
        return stageDefinitions.get(stageInConfiguration);
    }

    public void commitStage() {
        stageInConfiguration++;
    }

    public WarehousePathsManager getPathsManager() {
        return warehousePathsManagerFactory.create(stageInConfiguration);
    }

    public List<Stage> getStages(ContainerLauncher containerLauncher, List<String> runContainerCommands) {
        Function<StageDefinition, Stage> stageDefinitionReader = stageDefinitionReaderFactory.create(containerLauncher, runContainerCommands);
        return stageDefinitions.stream()
                .map(stageDefinitionReader)
                .collect(toList());
    }
}
