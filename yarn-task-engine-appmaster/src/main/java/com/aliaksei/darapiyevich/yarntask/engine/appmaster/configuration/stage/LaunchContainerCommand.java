package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage;

import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.DefinitionLocation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinitionSerializer;
import com.google.common.annotations.VisibleForTesting;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.springframework.yarn.am.container.ContainerLauncher;

import java.util.List;
import java.util.function.BiFunction;

@RequiredArgsConstructor
@EqualsAndHashCode(exclude = {"serializer", "fileSystem", "containerLauncher"})
@ToString(exclude = {"serializer", "fileSystem", "containerLauncher"})
public class LaunchContainerCommand {
    static final boolean OVERWRITE = true;

    private final TaskDefinition taskDefinition;
    private final TaskDefinitionSerializer serializer;
    private final String taskDefinitionLocation;
    private final FileSystem fileSystem;
    private final ContainerLauncher containerLauncher;
    private final List<String> runContainerCommands;
    private final BiFunction<String, List<String>, List<String>> addOptionToRunContainerCommand;

    public void execute(Container container) {
        saveTaskDefinition();
        launch(container);
    }

    @SneakyThrows
    private void saveTaskDefinition() {
        byte[] serialized = serializer.serialize(taskDefinition);
        FSDataOutputStream outputStream = fileSystem.create(toPath(taskDefinitionLocation), OVERWRITE);
        outputStream.write(serialized);
        outputStream.flush();
        outputStream.close();
    }

    @VisibleForTesting
    Path toPath(String taskDefinitionLocation) {
        return new Path(taskDefinitionLocation);
    }

    private void launch(Container container) {
        String taskDefinitionPointerArg = String.format("--%s=%s", DefinitionLocation.OPTION, taskDefinitionLocation);
        List<String> commandWithDefinitionLocationArg = addOptionToRunContainerCommand.apply(taskDefinitionPointerArg, runContainerCommands);
        containerLauncher.launchContainer(container, commandWithDefinitionLocationArg);
    }
}
