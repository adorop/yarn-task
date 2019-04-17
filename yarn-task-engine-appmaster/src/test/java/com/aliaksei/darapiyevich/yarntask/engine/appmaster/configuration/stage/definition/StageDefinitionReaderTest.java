package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.Stage;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.StagePathsDistributor;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManagerFactory;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinitionSerializer;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.yarn.am.container.ContainerLauncher;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class StageDefinitionReaderTest {
    private static final int PARALLELISM = 3;
    private final StageDefinition stageDefinition = new StageDefinition(0);

    @Mock
    private WarehousePathsManagerFactory warehousePathsManagerFactory;
    @Mock
    private WarehousePathsManager warehousePathsManager;
    @Mock
    private StagePathsDistributor stagePathsDistributor;
    @Mock
    private TaskDefinitionSerializer taskDefinitionSerializer;
    @Mock
    private FileSystem fileSystem;
    @Mock
    private ContainerLauncher containerLauncher;
    @Mock
    private BiFunction<String, List<String>, List<String>> addOptionToRunContainerCommand;

    private StageDefinitionReader reader;

    @Before
    public void setUp() throws Exception {
        initStageDefinition();
        initPathsManager();
        reader = new StageDefinitionReader(warehousePathsManagerFactory,
                stagePathsDistributor,
                taskDefinitionSerializer,
                fileSystem,
                containerLauncher,
                Collections.emptyList(),
                addOptionToRunContainerCommand
        );
    }

    private void initPathsManager() {
        when(warehousePathsManagerFactory.create(anyInt())).thenReturn(warehousePathsManager);
    }

    private void initStageDefinition() {
        stageDefinition.setParallelism(PARALLELISM);
        stageDefinition.setReaderDefinition(new ReaderDefinition());
        StageWriterDefinition stageWriterDefinition = getStageWriterDefinition();
        stageDefinition.setWriterDefinition(stageWriterDefinition);
        stageDefinition.setTransformerDefinitions(Collections.emptyList());
    }

    private StageWriterDefinition getStageWriterDefinition() {
        StageWriterDefinition stageWriterDefinition = new StageWriterDefinition();
        stageWriterDefinition.setPaths(getPaths());
        return stageWriterDefinition;
    }

    private List<String> getPaths() {
        return IntStream.range(0, PARALLELISM)
                .mapToObj(Integer::toString)
                .collect(toList());
    }

    @Test
    public void shouldReturnStageWithNumberOfLaunchContainerCommandsEqualToStageDefinitionParallelism() {
        Stage result = reader.apply(stageDefinition);
        assertThat(result.getLaunchContainerCommands(), hasSize(PARALLELISM));
    }
}