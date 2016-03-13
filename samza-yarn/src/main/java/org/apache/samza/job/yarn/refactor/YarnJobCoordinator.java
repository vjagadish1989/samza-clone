package org.apache.samza.job.yarn.refactor;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.samza.clustermanager.*;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.metrics.SamzaAppMasterMetrics;
import org.apache.samza.coordinator.JobModelReader;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Implements a JobCoordinator that runs Samza using Yarn as the cluster manager.
 * TODO:
 * 1.Make YarnJobCoordinator implement the JobCoordinator interface.
 * 2.Make JobModelReader conform to a standard interface.
 * 4.Get the process manager implementation from a factory
 */
public class YarnJobCoordinator implements ContainerProcessManagerCallback {

    private ContainerProcessManager processManager;
    private SamzaAppState state;
    private SamzaAppMasterMetrics metrics;
    private SamzaTaskManager taskManager;

    private JobModelReader jobModelReader;

    /**
     * Registry of components to register metrics to.
     */
    private MetricsRegistryMap registry;

    /*
     * The interval for polling the Task Manager for shutdown.
     */
    private long taskManagerPollInterval;

    /*
     * Config specifies if a Jmx server should be started on this Job Coordinator
     */
    private boolean isJmxEnabled;

    /**
     * Tracks the exception occuring in any callbacks from the ContainerProcessManager. Any errors from the
     * ContainerProcessManager will trigger shutdown of the YarnJobCoordinator.
     */
    private Throwable storedException;

    private static final Logger log = LoggerFactory.getLogger(YarnJobCoordinator.class);


    /**
     * Creates an YarnJobCoordinator instance providing a config object.
     * @param coordinatorSystemConfig the coordinator stream config that can be used to read the
     * {@link org.apache.samza.job.model.JobModel from.
     */
    public YarnJobCoordinator (Config coordinatorSystemConfig) {

        //instantiate data members
        jobModelReader = JobModelReader.apply(coordinatorSystemConfig, registry);
        log.info("Got coordinator system config: " + coordinatorSystemConfig);
        registry = new MetricsRegistryMap();
        Config config = jobModelReader.jobModel().getConfig();

        processManager = new YarnContainerManager(config, jobModelReader, this);
        state = new SamzaAppState(jobModelReader);
        metrics = new SamzaAppMasterMetrics(config, state, registry);
        taskManager = new SamzaTaskManager(config, state, processManager);

        YarnConfig yarnConfig = new YarnConfig(config);
        taskManagerPollInterval = yarnConfig.getAMPollIntervalMs();
        isJmxEnabled = yarnConfig.getJmxServerEnabled();
    }

    public void run() throws IOException {

        JmxServer jmxServer = null;

        if(isJmxEnabled) {
           jmxServer = new JmxServer();
           state.jmxUrl = jmxServer.getJmxUrl();
           state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
        }

        log.info("Starting Yarn Job Coordinator");

        try {
            processManager.start();
            metrics.start();
            taskManager.start();

            boolean isInterrupted = false;

            while (!taskManager.shouldShutdown() && !isInterrupted && storedException == null) {
                try {
                    Thread.sleep(taskManagerPollInterval);
                } catch (InterruptedException e) {
                    isInterrupted = true;
                    log.error("Interrupted in job coordinator loop {} ", e);
                    e.printStackTrace();
                }
            }
        }
        catch(Throwable e) {
            System.err.println(ExceptionUtils.getFullStackTrace(e));
            log.error("exception {} ", e);
            e.printStackTrace();
        }
        finally {
            metrics.stop();
            taskManager.stop();
            processManager.stop();
            if (jmxServer!=null) {
                jmxServer.stop();
            }
        }
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
    public void onResourcesCompleted(List<StreamProcessorStatus> resourceStatuses) {
        for(StreamProcessorStatus resourceStatus : resourceStatuses) {
            taskManager.onContainerCompleted(resourceStatus);
        }
    }

    @Override
    public void onError(Throwable e) {
         log.error("Stored exception : {}", e);
         storedException = e;
    }

    /**
     * The entry point for the {@link YarnJobCoordinator}
     * @param args
     * @throws IOException
     */
    public static void main (String args[]) throws IOException{
        Config coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG()), Config.class));
        log.info("Got coordinator system config:  " + coordinatorSystemConfig);
        YarnJobCoordinator jc = new YarnJobCoordinator(coordinatorSystemConfig);
        jc.run();
    }
}
