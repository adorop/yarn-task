package com.aliaksei.darapiyevich.yarntask.engine.contract.definition;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.io.InputStream;

public class TaskDefinitionSerializer {
    private final ObjectMapper mapper;

    public TaskDefinitionSerializer() {
        mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    @SneakyThrows
    public TaskDefinition deserialize(InputStream inputStream) {
        return mapper.readValue(inputStream, TaskDefinition.class);
    }

    @SneakyThrows
    public byte[] serialize(TaskDefinition taskDefinition) {
        return mapper.writeValueAsBytes(taskDefinition);
    }
}
