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
 * Implements a JobCoordinator that is completely independent of the underlying cluster
 * manager system. This {@link ClusterBasedJobCoordinator} handles functionality common
 * to both Yarn and Mesos. It takes care of
 *  1. Requesting resources from an underlying {@link ContainerProcessManager}.
 *  2. Ensuring that placement of containers to resources happens as per host affinity.
 *
 *  Any offer based cluster management system that must integrate with Samza will merely
 *  implement a {@link ContainerManagerFactory} and a {@link ContainerProcessManager}.
 *
 *  This class is not thread-safe, Hence, invocations should be synchronized by
 *  the callers.
 *
 * TODO:
 * 1. Refactor ContainerProcessManager to also handle process liveness, process start
 * callbacks
 * 2. Refactor the JobModelReader to be an interface.
 * 3. Make ClusterBasedJobCoordinator implement the JobCoordinator API as in SAMZA-881.
 * 4. Refactor UI state variables.
 * 5. Add another constructor that takes in a JobModelReader and a Config object.
 * 6. Unit tests.
 * 7. Documentation for the newly added configs.
 */
public class ClusterBasedJobCoordinator implements ContainerProcessManager.Callback {
  /**
   * A ContainerProcessManager takes care of requesting resources from a pool of resources
   */
  private ContainerProcessManager processManager;


  private SamzaAppState state;

  /**
   * Metrics to track stats around container failures, needed containers etc.
   */
  private SamzaAppMasterMetrics metrics;

  /**
   * Handles callback for allocated containers, failed containers.
   */
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

  private static final Logger log = LoggerFactory.getLogger(ClusterBasedJobCoordinator.class);

  private final Config coordinatorSystemConfig;

  private boolean isStarted;

  private JmxServer jmxServer;

  /**
   * Creates an YarnJobCoordinator instance providing a config object.
   *
   * @param coordinatorSystemConfig the coordinator stream config that can be used to read the
   *                                {@link org.apache.samza.job.model.JobModel from.
   */
  public ClusterBasedJobCoordinator(Config coordinatorSystemConfig)
  {
    this.coordinatorSystemConfig = coordinatorSystemConfig;
  }

  /**
   * Starts the JobCoordinator.
   *
   */
  public void start()
  {
    if (isStarted)
    {
        log.info("Attempting to start an already started job coordinator. ");
        return;
    }
    isStarted = true;

    try
    {
      registry = new MetricsRegistryMap();
      jobModelReader = JobModelReader.apply(coordinatorSystemConfig, registry);
      Config config = jobModelReader.jobModel().getConfig();

      ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
      taskManagerPollInterval = clusterManagerConfig.getJobCoordinatorSleepInterval();
      isJmxEnabled = clusterManagerConfig.getJmxServerEnabled();

      log.info("Got coordinator system config {} ", coordinatorSystemConfig);

      ContainerManagerFactory factory = getContainerProcessManagerFactory(clusterManagerConfig);
      this.processManager = factory.getContainerProcessManager(jobModelReader, this);

      state = new SamzaAppState(jobModelReader);
      metrics = new SamzaAppMasterMetrics(config, state, registry);
      taskManager = new SamzaTaskManager(config, state, processManager);


      if (isJmxEnabled) {
          jmxServer = new JmxServer();
          state.jmxUrl = jmxServer.getJmxUrl();
          state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
      }

      log.info("Starting Yarn Job Coordinator");

      processManager.start();
      metrics.start();
      taskManager.start();

      boolean isInterrupted = false;

      while (!taskManager.shouldShutdown() && !isInterrupted && storedException == null)
      {
        try {
          Thread.sleep(taskManagerPollInterval);
        }
        catch (InterruptedException e) {
          isInterrupted = true;
          log.error("Interrupted in job coordinator loop {} ", e);
          e.printStackTrace();
        }
      }
    }
    catch (Throwable e) {
        log.error("exception throw runtime {} ", e);
        e.printStackTrace();
        throw new SamzaException(e);
    }
    finally {
        //TODO: Prevent stop from being called multiple times or make methods resilient.
        stop();
    }
  }

  /**
   * Returns an instantiated {@link ContainerManagerFactory} from a {@link ClusterManagerConfig}
   * @param clusterManagerConfig, the cluster manager config to parse.
   *
   */
  private ContainerManagerFactory getContainerProcessManagerFactory(final ClusterManagerConfig clusterManagerConfig)
  {
    final String containerManagerFactoryClass = clusterManagerConfig.getContainerManagerClass();
    final ContainerManagerFactory factory;

    try
    {
      factory = (ContainerManagerFactory) Class.forName(containerManagerFactoryClass).newInstance();
    }
    catch (InstantiationException e) {
      log.error("Instantiation exception when creating ContainerManager", e);
      e.printStackTrace();
      throw new SamzaException(e);
    }
    catch (IllegalAccessException e) {
      log.error("Illegal access exception when creating ContainerManager", e);
      e.printStackTrace();
      throw new SamzaException(e);
    }
    catch (ClassNotFoundException e) {
      log.error("ClassNotFound Exception when creating ContainerManager", e);
      e.printStackTrace();
      throw new SamzaException(e);
    }
    return factory;
  }

  /**
   * Stops all components of the JobCoordinator.
   */
  public void stop() {
      if (metrics != null)
          metrics.stop();
      log.info("stopped metrics reporters");

      if (taskManager != null)
          taskManager.stop();
      log.info("stopped task manager");

      if (processManager != null)
          processManager.stop();
      log.info("stopped container process manager");

      if (jmxServer != null) {
          jmxServer.stop();
      }
      log.info("stopped Jmx Server");
  }

  /**
   * Delegate callback handling to the taskmanager
   * @param resources a list of available resources.
   */
  @Override
  public void onResourcesAvailable(List<SamzaResource> resources)
  {
      for (SamzaResource resource : resources) {
          taskManager.onContainerAllocated(resource);
      }
  }

  /**
   * Delegate callbacks of resource completion to the taskManager
   * @param resourceStatuses
   */
  @Override
  public void onResourcesCompleted(List<StreamProcessorStatus> resourceStatuses)
  {
      for (StreamProcessorStatus resourceStatus : resourceStatuses)
      {
          taskManager.onContainerCompleted(resourceStatus);
      }
  }

  /**
   * An error in the callback terminates the JobCoordinator
   * @param e
   */
  @Override
  public void onError(Throwable e)
  {
      log.error("Stored exception : {}", e);
      storedException = e;
  }

  /**
   * The entry point for the {@link ClusterBasedJobCoordinator}
   *
   * @param args
   * @throws IOException
   */
  public static void main(String args[])
  {
    Config coordinatorSystemConfig = null;
    final String COORDINATOR_SYSTEM_ENV = System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG());
    try
    {
      log.info("Parsing coordinator system config {}", COORDINATOR_SYSTEM_ENV);
      coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(COORDINATOR_SYSTEM_ENV, Config.class));
    }
    catch (IOException e)
    {
      e.printStackTrace();
      log.error("Exception while reading coordinator stream config {}", e);
      throw new SamzaException(e);
    }

    log.info("Got coordinator system config: {}  ", coordinatorSystemConfig);
    ClusterBasedJobCoordinator jc = new ClusterBasedJobCoordinator(coordinatorSystemConfig);
    jc.start();
  }
}
