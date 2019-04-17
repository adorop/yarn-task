package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.LaunchContainerCommand;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.Stage;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.StagePathsDistributor;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManagerFactory;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.*;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.yarn.am.container.ContainerLauncher;

import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toCollection;

@RequiredArgsConstructor
public class StageDefinitionReader implements Function<StageDefinition, Stage> {
    private final WarehousePathsManagerFactory warehousePathsManagerFactory;
    private final StagePathsDistributor stagePathsDistributor;
    private final TaskDefinitionSerializer taskDefinitionSerializer;
    private final FileSystem fileSystem;
    private final ContainerLauncher containerLauncher;
    private final List<String> runContainerCommands;
    private final BiFunction<String, List<String>, List<String>> addOptionToRunContainerCommand;

    @Override
    public Stage apply(StageDefinition stageDefinition) {
        Stage stage = new Stage();
        stage.setLaunchContainerCommands(readLaunchContainerCommands(stageDefinition));
        return stage;
    }

    private Queue<LaunchContainerCommand> readLaunchContainerCommands(StageDefinition stageDefinition) {
        return IntStream.range(0, stageDefinition.getParallelism())
                .mapToObj(getLaunchContainerCommand(stageDefinition))
                .collect(toCollection(ConcurrentLinkedQueue::new));
    }

    private IntFunction<LaunchContainerCommand> getLaunchContainerCommand(StageDefinition stageDefinition) {
        return containerIndex -> {
            TaskDefinition taskDefinition = getTaskDefinition(stageDefinition, containerIndex);
            WarehousePathsManager warehousePathsManager = warehousePathsManagerFactory.create(stageDefinition.getId());
            String definitionPath = warehousePathsManager.getDefinitionPath(containerIndex);
            return new LaunchContainerCommand(taskDefinition, taskDefinitionSerializer, definitionPath, fileSystem, containerLauncher, runContainerCommands, addOptionToRunContainerCommand);
        };
    }

    private TaskDefinition getTaskDefinition(StageDefinition stageDefinition, int containerIndex) {
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setReaderDefinition(getReaderDefinition(stageDefinition, containerIndex));
        taskDefinition.setTransformerDefinitions(stageDefinition.getTransformerDefinitions());
        taskDefinition.setWriterDefinition(getWriterDefinition(stageDefinition, containerIndex));
        return taskDefinition;
    }

    private ReaderDefinition getReaderDefinition(StageDefinition stageDefinition, int containerIndex) {
        ReaderDefinition taskReaderDefinition = new ReaderDefinition();
        ReaderDefinition stageReaderDefinition = stageDefinition.getReaderDefinition();
        taskReaderDefinition.setDataSchema(stageReaderDefinition.getDataSchema());
        stageReaderDefinition.getSelectSchema()
                .ifPresent(taskReaderDefinition::setSelectSchema);
        taskReaderDefinition.setFormat(stageReaderDefinition.getFormat());
        List<String> paths = stagePathsDistributor
                .getPerTaskPaths(stageReaderDefinition.getPaths(), stageDefinition.getParallelism(), containerIndex);
        taskReaderDefinition.setPaths(paths);
        return taskReaderDefinition;
    }

    private WriterDefinition getWriterDefinition(StageDefinition stageDefinition, int containerIndex) {
        WriterDefinition taskWriterDefinition = new WriterDefinition();
        StageWriterDefinition stageWriterDefinition = stageDefinition.getWriterDefinition();
        taskWriterDefinition.setFormat(stageWriterDefinition.getFormat());
        Optional<StageWriterPartitionsDefinition> stagePartitionsDefinition = stageWriterDefinition.getPartitionsDefinition();
        if (stagePartitionsDefinition.isPresent()) {
            PartitionsDefinition taskPartitionsDefinition =
                    getPartitionsDefinition(stageDefinition, containerIndex, stagePartitionsDefinition.get());
            taskWriterDefinition.setPartitionsDefinition(taskPartitionsDefinition);
        } else {
            taskWriterDefinition.setSinglePath(stageWriterDefinition.getPaths().get(containerIndex));
        }
        return taskWriterDefinition;
    }

    private PartitionsDefinition getPartitionsDefinition(StageDefinition stageDefinition,
                                                         int containerIndex,
                                                         StageWriterPartitionsDefinition stagePartitionsDefinition) {
        PartitionsDefinition taskPartitionsDefinition = new PartitionsDefinition();
        List<String> paths = stagePathsDistributor
                .getPerTaskPaths(stageDefinition.getWriterDefinition().getPaths(), stageDefinition.getParallelism(), containerIndex);
        taskPartitionsDefinition.setPaths(paths);
        taskPartitionsDefinition.setSchema(stagePartitionsDefinition.getSchema());
        taskPartitionsDefinition.setPartitionByKeys(stagePartitionsDefinition.getPartitionByKeys());
        return taskPartitionsDefinition;
    }
}
