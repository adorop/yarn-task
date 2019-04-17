package com.aliaksei.darapiyevich.yarntask.engine.appmaster.path;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class StagePathsDistributorTest {
    private static final int PARALLELISM = 3;
    private static final int TASK_INDEX = 1;
    private List<String> perStagePaths;

    private final StagePathsDistributor distributor = new StagePathsDistributor();

    @Before
    public void setUp() throws Exception {
        initPerStagePaths();
    }

    private void initPerStagePaths() {
        perStagePaths = IntStream.range(0, 9)
                .mapToObj(Integer::toString)
                .collect(toList());
    }

    @Test
    public void name() {
        List<String> perTaskPaths = distributor.getPerTaskPaths(perStagePaths, PARALLELISM, TASK_INDEX);
        assertThat(perTaskPaths, equalTo(getExpectedPerTaskPaths()));
    }

    private List<String> getExpectedPerTaskPaths() {
        return IntStream.range(3, 6)
                .mapToObj(Integer::toString)
                .collect(toList());
    }

    @Test
    public void shouldNotModifyInputPaths() {
        int initialPerStagePathsSize = perStagePaths.size();
        distributor.getPerTaskPaths(perStagePaths, PARALLELISM, TASK_INDEX);
        assertThat(perStagePaths, hasSize(initialPerStagePathsSize));
    }
}