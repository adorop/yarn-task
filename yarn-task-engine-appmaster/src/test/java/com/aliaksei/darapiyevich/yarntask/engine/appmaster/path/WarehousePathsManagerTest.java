package com.aliaksei.darapiyevich.yarntask.engine.appmaster.path;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class WarehousePathsManagerTest {
    private static final int CONTAINER_INDEX = 2;
    private static final String APPLICATION_NAME = "app";
    private static final int CURRENT_STAGE_PARALLELISM = 1;
    private static final int NEXT_STAGE_PARALLELISM = 2;
    private static final int STAGE_INDEX = 5;

    private List<String> expectedInterStagesPaths = Arrays.asList(
            WAREHOUSE_DIRECTORY + APPLICATION_NAME + "/" + INTER_STAGES_RESULTS_DIRECTORY + "stage_" + STAGE_INDEX + "/part_0_0",
            WAREHOUSE_DIRECTORY + APPLICATION_NAME + "/" + INTER_STAGES_RESULTS_DIRECTORY + "stage_" + STAGE_INDEX + "/part_0_1"
    );

    private WarehousePathsManager manager;

    @Before
    public void setUp() throws Exception {
        manager = new WarehousePathsManager(APPLICATION_NAME, STAGE_INDEX);
    }

    @Test
    public void getInterStagesPathsShouldReturnCartesianProductFromCurrentStageParallelismAndNextStageParallelism() {
        List<String> result = manager.getInterStagesPaths(CURRENT_STAGE_PARALLELISM, NEXT_STAGE_PARALLELISM);
        assertThat(result, equalTo(expectedInterStagesPaths));
    }

    @Test
    public void getDefinitionPathShouldReturnFileNameWithStageIndexAndContainerIndexInDefinitionsDirectoryOfGivenApplication() {
        String result = manager.getDefinitionPath(CONTAINER_INDEX);
        assertThat(result, equalTo(WAREHOUSE_DIRECTORY + APPLICATION_NAME + "/" + DEFINITIONS_DIRECTORY + STAGE_INDEX + "_" + CONTAINER_INDEX));
    }

    @Test
    public void getApplicationWarehouseDirectoryShouldReturnSubdirectoryOfWarehouseDirectoryWithApplicationName() {
        String result = WarehousePathsManager.getApplicationWarehouseDirectory(APPLICATION_NAME);
        assertThat(result, equalTo(WAREHOUSE_DIRECTORY + APPLICATION_NAME + "/"));
    }
}