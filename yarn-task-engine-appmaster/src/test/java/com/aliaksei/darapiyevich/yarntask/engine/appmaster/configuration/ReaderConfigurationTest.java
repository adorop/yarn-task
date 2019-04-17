package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReaderConfigurationTest {
    private final StageDefinition stageDefinition = new StageDefinition(0);

    @Mock
    private YarnApplication yarnApplication;
    @Mock
    private Type type;
    private Schema dataSchema;
    private Schema selectSchema = Schema.builder().build();

    private ReaderConfiguration readerConfiguration;

    @Before
    public void setUp() throws Exception {
        initSchemas();
        initYarnApplication();
        readerConfiguration = new ReaderConfiguration("format", yarnApplication)
                .inPath("paths", "to", "read")
                .withSchema(dataSchema);
    }

    private void initSchemas() {
        dataSchema = Schema.builder()
                .field(new Field("name", type))
                .build();
    }

    private void initYarnApplication() {
        when(yarnApplication.configure()).thenReturn(stageDefinition);
    }

    @Test
    public void shouldSetCurrentSchemaOfYarnApplicationToSelectSchema() {
        readerConfiguration.select();
        verify(yarnApplication).setCurrentSchema(selectSchema);
    }

    @Test
    public void shouldSetStageParallelismToCountOfPathsToReadFrom() {
        readerConfiguration.select();
        assertThat(stageDefinition.getParallelism(), is(3));
    }
}