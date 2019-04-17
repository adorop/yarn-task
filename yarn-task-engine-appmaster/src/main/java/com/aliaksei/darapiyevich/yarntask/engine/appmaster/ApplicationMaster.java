package com.aliaksei.darapiyevich.yarntask.engine.appmaster;

import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.YarnApplication;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.LaunchContainerCommand;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.stage.Stage;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.am.AbstractEventingAppmaster;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.AbstractAllocator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;
import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.FAILED;
import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;

@Slf4j
public class ApplicationMaster extends AbstractEventingAppmaster implements YarnAppmaster {
    private Iterator<Stage> stages;
    private Stage currentStage;

    private Map<ContainerId, LaunchContainerCommand> runningContainers = new ConcurrentHashMap<>();
    private Set<LaunchContainerCommand> failedCommands = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private AtomicInteger currentStageCompleteContainers = new AtomicInteger(0);
    private AtomicInteger currentStageContainersCount = new AtomicInteger(0);

    private YarnApplication yarnApplication;

    @Autowired
    public void setYarnApplication(YarnApplication yarnApplication) {
        this.yarnApplication = yarnApplication;
    }

    @Override
    protected void onInit() throws Exception {
        super.onInit();
        stages = yarnApplication.getStages(getLauncher(), getCommands()).iterator();
        currentStage = stages.next();
        currentStageContainersCount.set(currentStage.getLaunchContainerCommands().size());
    }

    @Override
    public void submitApplication() {
        registerAppmaster();
        if (getAllocator() instanceof AbstractAllocator) {
            ((AbstractAllocator) getAllocator()).setApplicationAttemptId(getApplicationAttemptId());
        }
        start();
        allocateContainers();
    }

    private void allocateContainers() {
        getAllocator().allocateContainers(currentStageContainersCount.get());
    }

    @Override
    protected void onContainerAllocated(Container container) {
        ofNullable(currentStage.getLaunchContainerCommands().poll())
                .ifPresent(trackAndExecute(container));
    }

    private Consumer<LaunchContainerCommand> trackAndExecute(Container container) {
        return command -> {
            runningContainers.put(container.getId(), command);
            command.execute(container);
        };
    }

    @Override
    protected void onContainerCompleted(ContainerStatus status) {
        log.info("Container completed: {}", status);
        int exitStatus = status.getExitStatus();
        ContainerId containerId = status.getContainerId();
        LaunchContainerCommand command = runningContainers.remove(containerId);
        if (failed(exitStatus)) {
            log.warn("Failed to execute command: {}, status: {}, diagnostics: {}", command, status.getExitStatus(), status.getDiagnostics());
            if (canRetry(command)) {
                retry(command);
            } else {
                failApplication(command);
            }
        } else {
            if (stageIsComplete()) {
                if (stages.hasNext()) {
                    startNewStage();
                } else {
                    finish();
                }
            }
        }
    }

    private boolean failed(int exitStatus) {
        return exitStatus != ContainerExitStatus.SUCCESS;
    }

    private boolean canRetry(LaunchContainerCommand command) {
        boolean can = !failedCommands.contains(command);
        failedCommands.add(command);
        return can;
    }

    private void failApplication(LaunchContainerCommand command) {
        log.error("Failed to execute command {}", command);
        setFinalApplicationStatus(FAILED);
        notifyCompleted();
    }

    private boolean stageIsComplete() {
        return currentStageCompleteContainers.incrementAndGet() == currentStageContainersCount.get();
    }

    private void startNewStage() {
        currentStage = stages.next();
        currentStageContainersCount.set(currentStage.getLaunchContainerCommands().size());
        currentStageCompleteContainers.set(0);
        log.info("Starting stage: {}", currentStage);
        allocateContainers();
    }

    private void finish() {
        setFinalApplicationStatus(SUCCEEDED);
        notifyCompleted();
    }

    private void retry(LaunchContainerCommand command) {
        currentStage.getLaunchContainerCommands().add(command);
        allocateContainer();
    }

    private void allocateContainer() {
        getAllocator().allocateContainers(1);
    }
}
