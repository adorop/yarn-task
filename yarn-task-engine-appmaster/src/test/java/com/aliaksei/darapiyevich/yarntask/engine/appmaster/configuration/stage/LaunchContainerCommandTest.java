package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage;

import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.DefinitionLocation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinitionSerializer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.yarn.am.container.ContainerLauncher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import static com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.LaunchContainerCommand.OVERWRITE;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LaunchContainerCommandTest {
    private static final String TASK_DEFINITION_LOCATION = "location";

    private final TaskDefinition taskDefinition = new TaskDefinition();
    private byte[] serializedTaskDefinition = new byte[]{1, 2, 3};
    private final List<String> runContainerCommands = Arrays.asList("run", "container");
    private final List<String> runContainerCommandsWithOption = Arrays.asList("with", "option");

    @Mock
    private Path path;
    @Mock
    private Container container;
    @Mock
    private FileSystem fileSystem;
    @Mock
    private TaskDefinitionSerializer serializer;
    @Mock
    private ContainerLauncher containerLauncher;
    @Mock
    private FSDataOutputStream outputStream;
    @Mock
    private BiFunction<String, List<String>, List<String>> addOptionToRunContainerCommand;

    private LaunchContainerCommand command;

    @Before
    public void setUp() throws Exception {
        initFileSystem();
        initSerializer();
        initCommand();
        command = new LaunchContainerCommand(taskDefinition,
                serializer,
                TASK_DEFINITION_LOCATION,
                fileSystem,
                containerLauncher,
                runContainerCommands,
                addOptionToRunContainerCommand
        ) {
            @Override
            Path toPath(String taskDefinitionLocation) {
                return path;
            }
        };
    }

    private void initSerializer() {
        when(serializer.serialize(taskDefinition)).thenReturn(serializedTaskDefinition);
    }

    private void initFileSystem() throws IOException {
        when(fileSystem.create(path, OVERWRITE)).thenReturn(outputStream);
    }

    private void initCommand() {
        String definitionLocationOption = "--" + DefinitionLocation.OPTION + "=" + TASK_DEFINITION_LOCATION;
        when(addOptionToRunContainerCommand.apply(definitionLocationOption, runContainerCommands))
                .thenReturn(runContainerCommandsWithOption);
    }

    @Test
    public void shouldSaveTaskDefinitionUnderGivenPath() throws IOException {
        command.execute(container);
        verify(outputStream).write(serializedTaskDefinition);
    }

    @Test
    public void shouldLaunchContainerWithPointerToDefinitionLocationAsArgument() {
        command.execute(container);
        verify(containerLauncher).launchContainer(container, runContainerCommandsWithOption);
    }
}