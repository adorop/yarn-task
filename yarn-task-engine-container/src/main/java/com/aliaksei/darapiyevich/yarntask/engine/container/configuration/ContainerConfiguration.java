package com.aliaksei.darapiyevich.yarntask.engine.container.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamReader;
import com.aliaksei.darapiyevich.yarntask.engine.container.StreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.container.Task;
import com.aliaksei.darapiyevich.yarntask.engine.container.Writer;
import com.aliaksei.darapiyevich.yarntask.engine.container.configuration.factory.StreamReadersFactory;
import com.aliaksei.darapiyevich.yarntask.engine.container.configuration.factory.StreamTransformerFactory;
import com.aliaksei.darapiyevich.yarntask.engine.container.configuration.factory.WriterFactory;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.DefinitionLocation;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinition;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinitionSerializer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

@Configuration
@ComponentScan("com.aliaksei.darapiyevich.yarntask.engine.container.configuration")
public class ContainerConfiguration {
    @Value("${spring.hadoop.fsUri}")
    private String hdfsUri;

    @Bean
    Task task(ArrayList<StreamReader> readers,
              StreamTransformer streamTransformer,
              Writer writer) {
        return new Task(readers, streamTransformer, writer);
    }

    @Bean
    FileSystem fileSystem() throws IOException {
        return FileSystem.get(URI.create(hdfsUri), new org.apache.hadoop.conf.Configuration());
    }

    @Bean
    TaskDefinitionSerializer stageDefinitionSerializer() {
        return new TaskDefinitionSerializer();
    }

    @Bean
    TaskDefinition taskDefinition(@Value("${" + DefinitionLocation.OPTION + "}") String stageDefinitionLocation,
                                  FileSystem fileSystem,
                                  TaskDefinitionSerializer taskDefinitionSerializer) throws IOException {
        FSDataInputStream is = fileSystem.open(new Path(stageDefinitionLocation));
        TaskDefinition taskDefinition = taskDefinitionSerializer.deserialize(is);
        is.close();
        return taskDefinition;
    }


    @Bean
    ArrayList<StreamReader> readers(TaskDefinition taskDefinition,
                                    FileSystem fileSystem) {
        return new ArrayList<>(new StreamReadersFactory(fileSystem).create(taskDefinition));
    }

    @Bean
    StreamTransformer streamTransformer(TaskDefinition taskDefinition) {
        return new StreamTransformerFactory().create(taskDefinition);
    }


    @Bean
    Writer writer(TaskDefinition taskDefinition,
                  FileSystem fileSystem) {
        return new WriterFactory(fileSystem).create(taskDefinition);
    }
}
