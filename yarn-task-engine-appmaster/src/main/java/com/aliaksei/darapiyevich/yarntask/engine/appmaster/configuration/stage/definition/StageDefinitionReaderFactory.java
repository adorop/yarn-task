package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.Stage;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.StagePathsDistributor;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManagerFactory;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinitionSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.yarn.am.container.ContainerLauncher;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

@RequiredArgsConstructor
public class StageDefinitionReaderFactory {
    private final WarehousePathsManagerFactory warehousePathsManagerFactory;
    private final StagePathsDistributor stagePathsDistributor;
    private final TaskDefinitionSerializer taskDefinitionSerializer;
    private final FileSystem fileSystem;
    private final BiFunction<String, List<String>, List<String>> addOptionToRunContainerCommand;

    public Function<StageDefinition, Stage> create(ContainerLauncher containerLauncher, List<String> runContainerCommands) {
        return new StageDefinitionReader(
                warehousePathsManagerFactory,
                stagePathsDistributor,
                taskDefinitionSerializer,
                fileSystem,
                containerLauncher,
                runContainerCommands,
                addOptionToRunContainerCommand
        );
    }
}
