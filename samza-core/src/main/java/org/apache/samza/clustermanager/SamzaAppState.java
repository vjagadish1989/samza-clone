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

import org.apache.samza.coordinator.JobCoordinator;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SamzaAppState {
  public final JobCoordinator jobCoordinator;

  /**
   * The following state variables are required for the correct functioning of the TaskManager
   * Some of them are shared between the AMRMCallbackThread and the ContainerAllocator thread, as mentioned below.
   */

  /**
   * Number of containers that have completed their execution and exited successfully
   */
  public AtomicInteger completedContainers = new AtomicInteger(0);

  /**
   * Number of failed containers
   * */
  public AtomicInteger failedContainers = new AtomicInteger(0);

  /**
   * Number of containers released due to extra allocation returned by the RM
   */
  public AtomicInteger releasedContainers = new AtomicInteger(0);

  /**
   * ContainerStatus of failed containers.
   */
  public ConcurrentMap<String, SamzaResourceStatus> failedContainersStatus = new ConcurrentHashMap<String, SamzaResourceStatus>();

  /**
   * Number of containers configured for the job
   */
  public int containerCount = 0;

  /**
   * Set of finished containers - TODO: Can be changed to a counter
   */
  public Set<Integer> finishedContainers = new HashSet<Integer>();

  /**
   *  Number of containers needed for the job to be declared healthy
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public AtomicInteger neededContainers = new AtomicInteger(0);

  /**
   *  Map of the samzaContainerId to the {@link SamzaResource} on which it is running
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public ConcurrentMap<Integer, SamzaResource> runningContainers = new ConcurrentHashMap<Integer, SamzaResource>(0);



  /**
   *  Map of the samzaContainerId to the {@link YarnContainer} on which it is running
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  /**
   * Final status of the application
   * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public String status = "undefined";

  /**
   * State indicating whether the job is healthy or not
   * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public AtomicBoolean jobHealthy = new AtomicBoolean(true);

  public AtomicInteger containerRequests = new AtomicInteger(0);

  public AtomicInteger matchedContainerRequests = new AtomicInteger(0);

  public SamzaAppState(JobCoordinator jobCoordinator
                      ) {
    this.jobCoordinator = jobCoordinator;

  }
}
