package com.aliaksei.darapiyevich.yarntask.container;

import com.aliaksei.darapiyevich.yarntask.engine.container.configuration.EnableYarnContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableYarnContainer
public class ContainerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ContainerApplication.class, args);
    }
}
