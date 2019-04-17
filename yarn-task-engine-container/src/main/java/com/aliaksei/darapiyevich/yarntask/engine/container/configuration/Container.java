package com.aliaksei.darapiyevich.yarntask.engine.container.configuration;

import com.aliaksei.darapiyevich.yarntask.engine.container.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;

@YarnComponent
public class Container {
    private final Task task;

    @Autowired
    public Container(Task task) {
        this.task = task;
    }

    @OnContainerStart
    public void start() {
        task.execute();
    }
}
