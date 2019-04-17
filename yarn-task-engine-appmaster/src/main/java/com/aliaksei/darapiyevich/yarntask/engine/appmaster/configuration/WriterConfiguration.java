package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageWriterDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.ResultPathManager;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class WriterConfiguration {
    private final YarnApplication yarnApplication;
    private final String path;
    private String format;

    public WriterConfiguration format(String format) {
        this.format = format;
        return this;
    }

    public YarnApplication write() {
        StageWriterDefinition writerDefinition = new StageWriterDefinition();
        writerDefinition.setPaths(getPaths());
        writerDefinition.setFormat(format);
        yarnApplication.configure().setWriterDefinition(writerDefinition);
        return yarnApplication;
    }

    private List<String> getPaths() {
        int parallelism = yarnApplication.configure().getParallelism();
        ResultPathManager resultPathManager = new ResultPathManager(parallelism);
        return resultPathManager.getFilesPaths(path);
    }
}
