/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements a JobCoordinator that is completely independent of the underlying cluster
 * manager system. This {@link ClusterBasedJobCoordinator} handles functionality common
 * to both Yarn and Mesos. It takes care of
 *  1. Requesting resources from an underlying {@link ContainerProcessManager}.
 *  2. Ensuring that placement of containers to resources happens (as per whether host affinity
 *  is configured or not).
 *
 *  Any offer based cluster management system that must integrate with Samza will merely
 *  implement a {@link ContainerManagerFactory} and a {@link ContainerProcessManager}.
 *
 *  This class is not thread-safe. For safe access in multi-threaded context, invocations
 *  should be synchronized by the callers.
 *
 * TODO:
 * 1. Refactor ContainerProcessManager to also handle process liveness, process start
 * callbacks
 * 2. Refactor the JobModelReader to be an interface.
 * 3. Make ClusterBasedJobCoordinator implement the JobCoordinator API as in SAMZA-881.
 * 4. Refactor UI state variables.
 * 5. Unit tests.
 * 6. Document newly added configs.
 */
public class ClusterBasedJobCoordinator implements ContainerProcessManager.Callback {
  private final Config config;
  private final ClusterManagerConfig clusterManagerConfig;
  /**
   * A ContainerProcessManager takes care of requesting resources from a pool of resources
   */
  private final ContainerProcessManager processManager;

  /**
   * State to track container failures, host-container mappings
   */
  private final SamzaAppState state;

  /**
   * Metrics to track stats around container failures, needed containers etc.
   */
  private final SamzaAppMasterMetrics metrics;

  //even though some of these can be converted to local variables, it will not be the case
  //as we add more methods to the JobCoordinator and completely implement SAMZA-881.

  /**
   * Handles callback for allocated containers, failed containers.
   */
  private final SamzaTaskManager taskManager;

  /**
   * A JobModelReader to return and refresh the {@link org.apache.samza.job.model.JobModel} when required.
   */
  private final JobModelReader jobModelReader;

  /*
   * The interval for polling the Task Manager for shutdown.
   */
  private final long taskManagerPollInterval;

  /*
   * Config specifies if a Jmx server should be started on this Job Coordinator
   */
  private final boolean isJmxEnabled;

  /**
   * Tracks the exception occuring in any callbacks from the ContainerProcessManager. Any errors from the
   * ContainerProcessManager will trigger shutdown of the YarnJobCoordinator.
   */
  private volatile boolean exceptionOccurred = false;

  private static final Logger log = LoggerFactory.getLogger(ClusterBasedJobCoordinator.class);

  /**
   * Internal boolean to check if the job coordinator has already been started.
   */
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  private JmxServer jmxServer;


  /**
   * Creates a new ClusterBasedJobCoordinator instance from a config. Invoke run() to actually
   * run the jobcoordinator.
   *
   * @param coordinatorSystemConfig the coordinator stream config that can be used to read the
   *                                {@link org.apache.samza.job.model.JobModel from.
   */
  public ClusterBasedJobCoordinator(Config coordinatorSystemConfig) {
    //TODO1: A couple of classes - namely
    //  1.JobCoordinator (jobModelReader in the new case)
    //  2.JmxServer
    // follow this weird pattern where their components are *started* in the constructor.
    // For example, the JmxServer class starts up the jmxServer in the constructor instead
    // of just defining a separate start method. This makes the life-cycle hard to manage.
    // (for example, consider a class X that includes a JmxServer member (in addition to several others)
    // and instantiates a JmxServer in its constructor. Then class X must ensure:
    // 1.jmxServer.close is called when constructor of class X fails due to some other reason unrelated to JmxServer
    // 2.jmxServer.close is called when class X's lifecycle ends. (during a clean shutdown)
    // this leads to buggy code in class X as class X has to call jmxServer.close() in 2 places.

    //TODO2: Re-design the JobCoordinator (JobModelReader now) class.
    //i) Decouple the exposing of the JobModel from building the JobModel. (Move the http server to another class)
    //Once both the above are completed, the new constructor will look like ClusterBasedJobCoordinator(JobModelReaderInterface reader).


    MetricsRegistryMap registry = new MetricsRegistryMap();
    jobModelReader = JobModelReader.apply(coordinatorSystemConfig, registry);;
    config = jobModelReader.jobModel().getConfig();

    state = new SamzaAppState(jobModelReader);

    clusterManagerConfig = new ClusterManagerConfig(config);
    isJmxEnabled = clusterManagerConfig.getJmxEnabled();

    ContainerManagerFactory factory = getContainerProcessManagerFactory(clusterManagerConfig);
    processManager = factory.getContainerProcessManager(jobModelReader, this, state);
    taskManagerPollInterval = clusterManagerConfig.getJobCoordinatorSleepInterval();

    metrics = new SamzaAppMasterMetrics(config, state, registry);
    taskManager = new SamzaTaskManager(config, state, processManager);
  }


  /**
   * Starts the JobCoordinator.
   *
   */
  public void run() {
    if(!isStarted.compareAndSet(false, true)){
      log.info("Attempting to start an already started job coordinator. ");
      return;
    }

    if (isJmxEnabled) {
      jmxServer = new JmxServer();
      state.jmxUrl = jmxServer.getJmxUrl();
      state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
    } else {
      jmxServer = null;
    }

    try {
      //initialize JobCoordinator state
      log.info("Starting Yarn Job Coordinator");

      processManager.start();
      metrics.start();
      taskManager.start();

      boolean isInterrupted = false;

      while (!taskManager.shouldShutdown() && !isInterrupted && !exceptionOccurred) {
        try {
          Thread.sleep(taskManagerPollInterval);
        }
        catch (InterruptedException e) {
          isInterrupted = true;
          log.error("Interrupted in job coordinator loop {} ", e);
          Thread.currentThread().interrupt();
        }
      }
    }
    catch (Throwable e) {
        log.error("Exception thrown in the JobCoordinator loop {} ", e);
        throw new SamzaException(e);
    }
    finally {
        onShutDown();
    }
  }

  /**
   * Returns an instantiated {@link ContainerManagerFactory} from a {@link ClusterManagerConfig}. The
   * {@link ContainerManagerFactory} is used to return an implementation of a {@link ContainerProcessManager}
   *
   * @param clusterManagerConfig, the cluster manager config to parse.
   *
   */
  private ContainerManagerFactory getContainerProcessManagerFactory(final ClusterManagerConfig clusterManagerConfig)
  {
    final String containerManagerFactoryClass = clusterManagerConfig.getContainerManagerClass();
    final ContainerManagerFactory factory;

    try {
      factory = (ContainerManagerFactory) Class.forName(containerManagerFactoryClass).newInstance();
    }
    catch (InstantiationException e) {
      log.error("Instantiation exception when creating ContainerManager", e);
      throw new SamzaException(e);
    }
    catch (IllegalAccessException e) {
      log.error("Illegal access exception when creating ContainerManager", e);
      throw new SamzaException(e);
    }
    catch (ClassNotFoundException e) {
      log.error("ClassNotFound Exception when creating ContainerManager", e);
      throw new SamzaException(e);
    }
    return factory;
  }

  /**
   * Stops all components of the JobCoordinator.
   */
  private void onShutDown() {
    if (metrics != null) {
      try {
        metrics.stop();
      }
      catch(Throwable e) {
        log.error("Exception while stopping metrics {}", e);
      }
      log.info("Stopped metrics reporters");
    }

    if (taskManager != null) {
      try {
        taskManager.stop();
      }
      catch(Throwable e) {
        log.error("Exception while stopping task manager {}", e);
      }
      log.info("Stopped task manager");
    }

    if (processManager != null) {
      try {
        processManager.stop(state.status);
      }
      catch(Throwable e) {
        log.error("Exception while stopping process manager {}", e);
      }
      log.info("Stopped container process manager");
    }

    if (jmxServer != null) {
      try {
        jmxServer.stop();
        log.info("Stopped Jmx Server");
      }
      catch(Throwable e) {
        log.error("Exception while stopping jmx server {}", e);
      }
    }
  }

  /**
   * Called by the {@link ContainerProcessManager} when there are resources available.
   * This delegates handling of the callbacks to the {@link SamzaTaskManager}
   * @param containers a list of available resources.
   */
  @Override
  public void onResourcesAvailable(List<SamzaResource> containers) {
      for (SamzaResource container : containers) {
          taskManager.onContainerAllocated(container);
      }
  }

  /**
   *
   * Delegate callbacks of resource completion to the taskManager
   * @param containerStatuses the statuses for the resources that have completed
   */
  @Override
  public void onResourcesCompleted(List<SamzaResourceStatus> containerStatuses) {
      for (SamzaResourceStatus containerStatus : containerStatuses) {
          taskManager.onContainerCompleted(containerStatus);
      }
  }

  /**
   * An error in the callback terminates the JobCoordinator
   * @param e the underlying exception/error
   */
  @Override
  public void onError(Throwable e) {
      log.error("Exception occured in callbacks from the ContainerManager : {}", e);
      exceptionOccurred = true;
  }

  /**
   * The entry point for the {@link ClusterBasedJobCoordinator}
   *
   */
  public static void main(String args[]) {
    Config coordinatorSystemConfig = null;
    final String COORDINATOR_SYSTEM_ENV = System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG());
    try {
      //Read and parse the coordinator system config.
      log.info("Parsing coordinator system config {}", COORDINATOR_SYSTEM_ENV);
      coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(COORDINATOR_SYSTEM_ENV, Config.class));
    } catch (IOException e) {
      log.error("Exception while reading coordinator stream config {}", e);
      throw new SamzaException(e);
    }
    ClusterBasedJobCoordinator jc = new ClusterBasedJobCoordinator(coordinatorSystemConfig);
    jc.run();
  }
}
