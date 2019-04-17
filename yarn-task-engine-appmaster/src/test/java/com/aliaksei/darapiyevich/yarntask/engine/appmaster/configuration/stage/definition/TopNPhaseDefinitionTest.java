package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TopNPhaseDefinitionTest {
    private static final int CURRENT_STAGE_PARALLELISM = 5;
    private static final String COLUMN = "column";
    private static final int LIMIT = 2;

    @Mock
    private Schema schema;
    @Mock
    private WarehousePathsManager pathsManager;

    private TopNPhaseDefinition definition;

    @Before
    public void setUp() throws Exception {
        definition = new TopNPhaseDefinition(
                schema,
                LIMIT,
                COLUMN,
                pathsManager,
                CURRENT_STAGE_PARALLELISM
        );
    }

    @Test
    public void newSchemaShouldAbsent() {
        Optional<Schema> schema = definition.newSchema();
        assertFalse(schema.isPresent());
    }

    @Test
    public void shouldSetReaderAndWriterPathsToSameValues() {
        ReaderDefinition reader = definition.nextStageReader();
        StageWriterDefinition writer = definition.writer();
        assertThat(reader.getPaths(), equalTo(writer.getPaths()));
    }

    @Test
    public void shouldSetReaderAndWriterFormatToSameValue() {
        ReaderDefinition reader = definition.nextStageReader();
        StageWriterDefinition writer = definition.writer();
        assertThat(reader.getFormat(), equalTo(writer.getFormat()));
    }

    @Test
    public void nextStageParallelismShouldBeOne() {
        int nextStageParallelism = definition.nextStageParallelism();
        assertThat(nextStageParallelism, is(1));
    }
}