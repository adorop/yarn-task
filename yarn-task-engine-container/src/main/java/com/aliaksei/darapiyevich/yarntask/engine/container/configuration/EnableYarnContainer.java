package com.aliaksei.darapiyevich.yarntask.engine.container.configuration;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({ContainerConfiguration.class})
public @interface EnableYarnContainer {
}
