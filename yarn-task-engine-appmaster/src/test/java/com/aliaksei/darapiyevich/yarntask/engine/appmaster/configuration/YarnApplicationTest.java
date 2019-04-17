package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageDefinitionReaderFactory;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManagerFactory;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class YarnApplicationTest {
    private static final StageDefinition EMPTY_STAGE_DEFINITION = new StageDefinition(0);
    @Mock
    private WarehousePathsManagerFactory warehousePathsManagerFactory;
    @Mock
    private StageDefinitionReaderFactory stageDefinitionReaderFactory;

    private YarnApplication yarnApplication;

    @Before
    public void setUp() throws Exception {
        yarnApplication = new YarnApplication(warehousePathsManagerFactory, stageDefinitionReaderFactory);
    }

    @Test
    public void configureShouldReturnEmptyStageDefinitionWhenHasNotBeenConfigured() {
        StageDefinition result = yarnApplication.configure();
        assertThat(result, equalTo(EMPTY_STAGE_DEFINITION));
    }

    @Test
    public void configureShouldReturnConfiguredStageDefinitionWhenItHasBeenConfigured() {
        ReaderDefinition readerDefinition = new ReaderDefinition();
        yarnApplication.configure().setReaderDefinition(readerDefinition);
        assertThat(yarnApplication.configure().getReaderDefinition(), equalTo(readerDefinition));
    }

    @Test
    public void configureShouldReturnEmptyAfterStageHasBeenCommited() {
        yarnApplication.configure().setReaderDefinition(new ReaderDefinition());
        yarnApplication.commitStage();
        assertThat(yarnApplication.configure().getReaderDefinition(), is(nullValue()));
    }
}