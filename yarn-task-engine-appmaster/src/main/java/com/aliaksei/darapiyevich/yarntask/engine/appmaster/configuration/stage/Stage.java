package com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage;

import lombok.Data;

import java.util.Queue;

@Data
public class Stage {
    private Queue<LaunchContainerCommand> launchContainerCommands;
}
