package org.apache.samza.coordinator;

import org.apache.samza.clustermanager.*;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.job.coordinator.SamzaAppMasterMetrics;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by jvenkatr on 2/4/16.
 */
public class YarnJobCoordinator implements ContainerProcessManagerCallback {

    ContainerProcessManager processManager;
    SamzaAppState state;
    SamzaAppMasterMetrics metrics;
    SamzaTaskManager taskManager;

    private static final Logger log = LoggerFactory.getLogger(YarnJobCoordinator.class);
    private Exception storedException;


    public void run() throws IOException {
        final int DEFAULT_POLL_INTERVAL_MS = 1000;

        MetricsRegistryMap registry = new MetricsRegistryMap();
        try {
            Config coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG()), Config.class));
            log.info("Got coordinator system config:  " + coordinatorSystemConfig);
            JobCoordinator jobCoordinator = JobCoordinator.apply(coordinatorSystemConfig, registry);

            Config config = jobCoordinator.jobModel().getConfig();
            log.info("Got config: " + coordinatorSystemConfig);

            // get this from factory.
            processManager = new YarnContainerManager(config, jobCoordinator, this);

            state = new SamzaAppState(jobCoordinator);
            metrics = new SamzaAppMasterMetrics(config, state, registry);
            taskManager = new SamzaTaskManager(config, state, processManager);

            processManager.start();
            metrics.onInit();
            taskManager.onInit();

            //TODO: move this out of yarn config
            YarnConfig yarnConfig = new YarnConfig(config);
            long interval = yarnConfig.getAMPollIntervalMs();

            boolean isShutdown = false;

            while (!taskManager.shouldShutdown() && !isShutdown) {

                try {
                    Thread.sleep(interval);
                } catch (Exception e) {
                    isShutdown = true;
                    e.printStackTrace();
                }

            }
        }
        catch(Exception e) {

            log.error("exception {} ", e);
            e.printStackTrace();
        }
        finally {
            metrics.onShutdown();
            taskManager.onShutdown();
            processManager.stop();

        }



    }

    public static void main (String args[]) throws IOException{
        YarnJobCoordinator jc = new YarnJobCoordinator();
        jc.run();
    }

    @Override
    public void onResourcesAvailable(List<SamzaResource> resources) {
        for(SamzaResource resource : resources) {
            taskManager.onContainerAllocated(resource);
        }
    }

    @Override
    public void onResourcesWithdrawn(List<SamzaResource> resources) {

    }

    @Override
    public void onResourcesCompleted(List<SamzaResourceStatus> resourceStatuses) {
        for(SamzaResourceStatus resourceStatus : resourceStatuses) {
            taskManager.onContainerCompleted(resourceStatus);
        }
    }

    @Override
    public void onError(Exception e) {
         log.error("Stored exception : {}", e);
         storedException = e;
    }
}
