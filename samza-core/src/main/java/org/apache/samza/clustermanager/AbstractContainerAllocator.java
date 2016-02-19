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

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.ShellCommandBuilder;
import org.apache.samza.util.Util;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is responsible for making requests for containers to the AM and also, assigning a container to run on an allocated resource.
 *
 * Since we are using a simple thread based allocation of a container to an allocated resource, the subclasses should implement {@link Runnable} interface.
 * The allocator thread follows the lifecycle of the {@link SamzaTaskManager}. Depending on whether host-affinity is enabled or not, the allocation model varies.
 *
 * See {@link ContainerAllocator} and {@link HostAwareContainerAllocator}
 */
public abstract class AbstractContainerAllocator implements Runnable {
  public static final String ANY_HOST = ContainerRequestState.ANY_HOST;
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_CONTAINER_MEM = 1024;
  public static final int DEFAULT_CPU_CORES = 1;

  protected final ContainerProcessManager amClient;
  protected final int ALLOCATOR_SLEEP_TIME;
  protected final int containerMaxMemoryMb;
  protected final int containerMaxCpuCore;
  private final TaskConfig taskConfig;
  Config config = null;
  SamzaAppState state;

  @Override
  public abstract void run();

  // containerRequestState indicate the state of all unfulfilled container requests and allocated containers
  protected final ContainerRequestState containerRequestState;

  // state that controls the lifecycle of the allocator thread
  protected AtomicBoolean isRunning = new AtomicBoolean(true);

  public AbstractContainerAllocator(ContainerProcessManager amClient,
                                    ContainerRequestState containerRequestState,
                                    Config config, SamzaAppState state) {
    JobConfig jobConfig = new JobConfig(config);
    this.amClient = amClient;
    this.ALLOCATOR_SLEEP_TIME = jobConfig.getAllocatorSleepTime();
    this.containerRequestState = containerRequestState;
    this.containerMaxMemoryMb = jobConfig.getContainerMemoryMb();
    this.containerMaxCpuCore = jobConfig.getNumCores();
    this.taskConfig = new TaskConfig(config);
    this.state = state;
  }


  /**
   * Called during initial request for containers
   *
   * @param containerToHostMappings Map of containerId to its last seen host (locality).
   *                                The locality value is null, either
   *                                - when host-affinity is not enabled, or
   *                                - when host-affinity is enabled and job is run for the first time
   */
  public void requestContainers(Map<Integer, String> containerToHostMappings) {
    for (Map.Entry<Integer, String> entry : containerToHostMappings.entrySet()) {
      int containerId = entry.getKey();
      String preferredHost = entry.getValue();
      if (preferredHost == null)
        preferredHost = ANY_HOST;

      requestContainer(containerId, preferredHost);
    }
  }
  /**
   * Method to request a container resource from yarn
   *
   * @param expectedContainerId Identifier of the container that will be run when a container resource is allocated for
   *                            this request
   * @param preferredHost Name of the host that you prefer to run the container on
   */
  public final void requestContainer(int expectedContainerId, String preferredHost) {
    //TODO: give a legit req id
    SamzaResourceRequest request = new SamzaResourceRequest(this.containerMaxCpuCore, this.containerMaxMemoryMb, preferredHost, UUID.randomUUID().toString() ,expectedContainerId);
    List<SamzaResourceRequest> requests = new ArrayList<SamzaResourceRequest>(Collections.<SamzaResourceRequest>singletonList(request));
    containerRequestState.addResourceRequest(requests);
    state.containerRequests.incrementAndGet();
  }


  public CommandBuilder getCommandBuilder(int samzaContainerId) {
    String cmdBuilderClassName;
    if (taskConfig.getCommandClass().isDefined()) {
      cmdBuilderClassName = taskConfig.getCommandClass().get();
    } else {
      cmdBuilderClassName = ShellCommandBuilder.class.getName();
    }
    CommandBuilder cmdBuilder = (CommandBuilder) Util.getObj(cmdBuilderClassName);
    cmdBuilder.setConfig(config).setId(samzaContainerId).setUrl(state.jobCoordinator.server().getUrl());
    return cmdBuilder;
  }
  /**
   * Method that adds allocated container to a synchronized buffer of allocated containers list
   * See allocatedContainers in {@link ContainerRequestState}
   *
   * @param container Container resource returned by the RM
   */
  public final void addContainer(SamzaResource container) {
    containerRequestState.addResource(container);
  }

  public final void setIsRunning(boolean state) {
    isRunning.set(state);
  }

}