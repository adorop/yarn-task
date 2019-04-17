package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.YarnApplication;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.AggregationTransformationDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TransformerDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TwoStagesPhaseConfigurerTest {
    private static final int NEXT_STAGE_PARALLELISM = 5;

    private final StageDefinition stageInConfiguration = new StageDefinition(0);
    private final StageDefinition nextStageInConfiguration = new StageDefinition(1);

    private TransformerDefinition transformation = new AggregationTransformationDefinition();
    private TransformerDefinition nextStageTransformation = new AggregationTransformationDefinition();
    private StageWriterDefinition writer = new StageWriterDefinition();
    private ReaderDefinition nextStageReader = new ReaderDefinition();

    @Mock
    private YarnApplication yarnApplication;
    @Mock
    private TwoStagesPhaseDefinition twoStagesPhaseDefinition;
    @InjectMocks
    private TwoStagesPhaseConfigurer configurer;

    @Before
    public void setUp() throws Exception {
        initStageInConfiguration();
        initDefinitions();
    }

    private void initStageInConfiguration() {
        when(yarnApplication.configure()).thenReturn(stageInConfiguration, nextStageInConfiguration);
    }

    private void initDefinitions() {
        when(twoStagesPhaseDefinition.transformation()).thenReturn(transformation);
        when(twoStagesPhaseDefinition.writer()).thenReturn(writer);
        when(twoStagesPhaseDefinition.nextStageReader()).thenReturn(nextStageReader);
        when(twoStagesPhaseDefinition.nextStageParallelism()).thenReturn(NEXT_STAGE_PARALLELISM);
        when(twoStagesPhaseDefinition.newSchema()).thenReturn(Optional.empty());
        when(twoStagesPhaseDefinition.nextStageTransformation()).thenReturn(nextStageTransformation);
        nextStageTransformation.setSchema(Schema.builder().build());
    }

    @Test
    public void shouldSetTransformationAndWriterToCurrentStageInConfiguration() {
        configurer.configure(yarnApplication);
        assertThat(stageInConfiguration.getTransformerDefinitions(), equalTo(Collections.singletonList(transformation)));
        assertThat(stageInConfiguration.getWriterDefinition(), equalTo(writer));
    }

    @Test
    public void shouldCommitStageAndSetReaderAndTransformationToNewOne() {
        configurer.configure(yarnApplication);
        verifyCommitsStage();
        assertThat(nextStageInConfiguration.getReaderDefinition(), is(nextStageReader));
        assertThat(nextStageInConfiguration.getTransformerDefinitions(), equalTo(Collections.singletonList(nextStageTransformation)));
    }

    private void verifyCommitsStage() {
        InOrder inOrder = inOrder(yarnApplication);
        inOrder.verify(yarnApplication).configure();
        inOrder.verify(yarnApplication).commitStage();
        inOrder.verify(yarnApplication).configure();
    }

    @Test
    public void shouldSetParallelismToNextStage() {
        configurer.configure(yarnApplication);
        assertThat(nextStageInConfiguration.getParallelism(), is(NEXT_STAGE_PARALLELISM));
    }

    @Test
    public void shouldSetNewSchemaToYarnApplicationWhenPresent() {
        Schema newSchema = getNewSchema();
        configurer.configure(yarnApplication);
        verify(yarnApplication).setCurrentSchema(newSchema);
    }

    private Schema getNewSchema() {
        Schema schema = Schema.builder().build();
        when(twoStagesPhaseDefinition.newSchema()).thenReturn(Optional.of(schema));
        return schema;
    }
}