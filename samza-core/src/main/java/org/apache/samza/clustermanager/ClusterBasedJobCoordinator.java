package org.apache.samza.clustermanager;

import org.apache.samza.SamzaException;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.coordinator.JobModelReader;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.SamzaAppMasterMetrics;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Implements a JobCoordinator that uses
 *
 */
public class ClusterBasedJobCoordinator implements ContainerProcessManagerCallback {

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
    private Exception storedException;

    private static final Logger log = LoggerFactory.getLogger(ClusterBasedJobCoordinator.class);

    private Config coordinatorSystemConfig ;

    private boolean isStarted;

    private JmxServer jmxServer;

    /**
     * Creates an YarnJobCoordinator instance providing a config object.
     * @param coordinatorSystemConfig the coordinator stream config that can be used to read the
     * {@link org.apache.samza.job.model.JobModel from.
     */
    public ClusterBasedJobCoordinator(Config coordinatorSystemConfig) {
          this.coordinatorSystemConfig = coordinatorSystemConfig;
         }

    private ContainerManagerFactory getContainerProcessManagerFactory(String containerProcessManagerFactory) {
       ContainerManagerFactory factory;
        try {
            factory = (ContainerManagerFactory) Class.forName(containerProcessManagerFactory).newInstance();
            factory.getContainerProcessManager(jobModelReader, this);
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new SamzaException(e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new SamzaException(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new SamzaException(e);
        }
        return factory;
    }

    public void start()  {
        if(isStarted) {
            log.info("Attempting to start an already started job coordinator. ");
            return;
        }


        isStarted = true;
        try {
        registry = new MetricsRegistryMap();
        jobModelReader = JobModelReader.apply(coordinatorSystemConfig, registry);
        Config config = jobModelReader.jobModel().getConfig();

        ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
        taskManagerPollInterval = clusterManagerConfig.getJobCoordinatorSleepInterval();
        isJmxEnabled = clusterManagerConfig.getJmxServerEnabled();

        //instantiate data members
        log.info("Got coordinator system config: " + coordinatorSystemConfig);

        String containerManagerFactoryClass = clusterManagerConfig.getContainerManagerClass();
        ContainerManagerFactory factory = getContainerProcessManagerFactory(containerManagerFactoryClass);
        this.processManager = factory.getContainerProcessManager(jobModelReader, this);

        //processManager = refactor YarnContainerManager(config, jobModelReader, this);
        state = new SamzaAppState(jobModelReader);
        metrics = new SamzaAppMasterMetrics(config, state, registry);
        taskManager = new SamzaTaskManager(config, state, processManager);


        if(isJmxEnabled) {
            jmxServer = new JmxServer();
            state.jmxUrl = jmxServer.getJmxUrl();
            state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
        }

        log.info("Starting Yarn Job Coordinator");

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
            log.error("exception throw runtime {} ", e);
            e.printStackTrace();
            throw new RuntimeException();
        }
        finally {
            //TODO: Prevent stop from being called multiple times or make methods resilient.
            stop();
        }
    }

    public void stop() {
        if(metrics!=null)
            metrics.stop();
        log.info("stopped 1");
        if(taskManager!=null)
            taskManager.stop();
        log.info("stopped 2");

        if(processManager!=null)
            processManager.stop();
        log.info("stopped 3");

        if (jmxServer!=null) {
            jmxServer.stop();
        }
        log.info("stopped 4");

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

    /**
     * The entry point for the {@link ClusterBasedJobCoordinator}
     * @param args
     * @throws IOException
     */
    public static void main (String args[]) {
        Config coordinatorSystemConfig = null;
        final String COORDINATOR_SYSTEM_ENV = System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG());
        try
        {
            log.info("Parsing {}", COORDINATOR_SYSTEM_ENV);
            coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(COORDINATOR_SYSTEM_ENV, Config.class));
        }
        catch (IOException e) {
            e.printStackTrace();
            log.error("Exception while reading coordinator stream config {}", e);
            throw new SamzaException(e);
        }

        log.info("Got coordinator system config: {}  ", coordinatorSystemConfig);
        ClusterBasedJobCoordinator jc = new ClusterBasedJobCoordinator(coordinatorSystemConfig);
        jc.start();
    }
}
