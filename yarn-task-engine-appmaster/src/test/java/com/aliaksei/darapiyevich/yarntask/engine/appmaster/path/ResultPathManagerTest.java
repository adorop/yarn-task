package com.aliaksei.darapiyevich.yarntask.engine.appmaster.path;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ResultPathManagerTest {
    private static final String DIRECTORY_WITHOUT_ENDING_SLASH = "/directory";
    private static final String DIRECTORY_WITH_ENDING_SLASH = "/directory/";
    private static final int PARALLELISM = 3;

    private final ResultPathManager manager = new ResultPathManager(PARALLELISM);


    @Test
    public void shouldAddEndingSlashToDirectoryNameWhenMissed() {
        List<String> filesPaths = manager.getFilesPaths(DIRECTORY_WITHOUT_ENDING_SLASH);
        assertThat(filesPaths, equalTo(getExpectedFilesPaths()));
    }

    private List<String> getExpectedFilesPaths() {
        return Arrays.asList(
                DIRECTORY_WITHOUT_ENDING_SLASH + "/" + "part_0",
                DIRECTORY_WITHOUT_ENDING_SLASH + "/" + "part_1",
                DIRECTORY_WITHOUT_ENDING_SLASH + "/" + "part_2"
        );
    }

    @Test
    public void shouldNotAddEndingSlashToDirectoryNameWhenPresents() {
        List<String> result = manager.getFilesPaths(DIRECTORY_WITH_ENDING_SLASH);
        assertThat(result, equalTo(getExpectedFilesPaths()));
    }
}