package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.ReaderDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.SchemaUtils;

import java.util.Arrays;

public class ReaderConfiguration {
    private final ReaderDefinition readerDefinition = new ReaderDefinition();
    private final YarnApplication yarnApplication;

    public ReaderConfiguration(String format, YarnApplication yarnApplication) {
        readerDefinition.setFormat(format);
        this.yarnApplication = yarnApplication;
    }


    public ReaderConfiguration inPath(String... files) {
        readerDefinition.setPaths(Arrays.asList(files));
        return this;
    }

    public ReaderConfiguration withSchema(Schema schema) {
        readerDefinition.setDataSchema(schema);
        return this;
    }

    public TransformationConfiguration select(String... columns) {
        Schema selectSchema = getSelectSchema(columns);
        readerDefinition.setSelectSchema(selectSchema);
        yarnApplication.setCurrentSchema(selectSchema);
        yarnApplication.configure().setReaderDefinition(readerDefinition);
        yarnApplication.configure().setParallelism(readerDefinition.getPaths().size());
        return new TransformationConfiguration(yarnApplication);
    }

    private Schema getSelectSchema(String[] columns) {
        return Schema.builder()
                .fields(SchemaUtils.selectFields(readerDefinition.getDataSchema(), columns))
                .build();
    }
}
