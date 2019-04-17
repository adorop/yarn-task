package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.AddOptionToRunContainerCommand;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.definition.StageDefinitionReaderFactory;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.StagePathsDistributor;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManager;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.path.WarehousePathsManagerFactory;
import com.aliaksei.darapiyevich.yarntask.engine.contract.definition.TaskDefinitionSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.function.BiFunction;

public abstract class YarnApplicationConfigurerAdapter {
    private static final boolean RECURSIVE = true;

    @Value("${spring.hadoop.fsUri}")
    private String hdfsUri;
    @Value("${spring.yarn.appName}")
    private String applicationName;
    @Value("${yarntask.warehouse.cleanup:false}")
    private boolean cleanupWarehouse;

    protected abstract YarnApplication configure(YarnApplicationConfigurer configurer);

    @Bean
    YarnApplication yarnApplication(YarnApplicationConfigurer configurer) {
        return configure(configurer);
    }

    @Bean
    YarnApplicationConfigurer yarnApplicationConfigurer(WarehousePathsManagerFactory warehousePathsManagerFactory,
                                                        StageDefinitionReaderFactory stageDefinitionReaderFactory) {
        return new YarnApplicationConfigurer(warehousePathsManagerFactory, stageDefinitionReaderFactory);
    }

    @Bean
    WarehousePathsManagerFactory warehousePathsManagerFactory() {
        return new WarehousePathsManagerFactory(applicationName);
    }


    @Bean
    StageDefinitionReaderFactory stageDefinitionReaderFactory(WarehousePathsManagerFactory warehousePathsManagerFactory,
                                                              StagePathsDistributor stagePathsDistributor,
                                                              TaskDefinitionSerializer taskDefinitionSerializer,
                                                              FileSystem fileSystem) {
        return new StageDefinitionReaderFactory(
                warehousePathsManagerFactory,
                stagePathsDistributor,
                taskDefinitionSerializer,
                fileSystem,
                addOptionToRunContainerCommand()
        );
    }

    private BiFunction<String, List<String>, List<String>> addOptionToRunContainerCommand() {
        return new AddOptionToRunContainerCommand();
    }

    @Bean
    StagePathsDistributor stagePathsDistributor() {
        return new StagePathsDistributor();
    }

    @Bean
    TaskDefinitionSerializer taskDefinitionSerializer() {
        return new TaskDefinitionSerializer();
    }

    @Bean
    FileSystem fileSystem() throws IOException {
        return FileSystem.get(URI.create(hdfsUri), new Configuration());
    }

    @PreDestroy
    void cleanupWarehouse() throws IOException {
        if (cleanupWarehouse) {
            String applicationWarehouseDirectory = WarehousePathsManager.getApplicationWarehouseDirectory(applicationName);
            fileSystem().delete(new Path(applicationWarehouseDirectory), RECURSIVE);
        }
    }
}


