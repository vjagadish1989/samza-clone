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
import org.apache.samza.job.CommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * This is the default allocator thread that will be used by SamzaTaskManager.
 *
 * When host-affinity is not enabled, this thread periodically wakes up to assign a container to an allocated resource.
 * If there aren't enough containers, it waits by sleeping for {@code ALLOCATOR_SLEEP_TIME} milliseconds.
 */
public class ContainerAllocator extends AbstractContainerAllocator {
  private static final Logger log = LoggerFactory.getLogger(ContainerAllocator.class);

  public ContainerAllocator(ContainerProcessManager manager,
                            Config config, SamzaAppState state) {
    super(manager, new ContainerRequestState(false, manager), config, state);
      }

  /**
   * During the run() method, the thread sleeps for ALLOCATOR_SLEEP_TIME ms. It tries to allocate any unsatisfied
   * request that is still in the request queue (See requests in {@link ContainerRequestState})
   * with allocated containers, if any.
   *
   * Since host-affinity is not enabled, all allocated container resources are buffered in the list keyed by "ANY_HOST".
   * */
  @Override
  public void assignContainerRequests() {
    while (hasPendingRequest() && hasAllocatedContainer(ANY_HOST)) {
      SamzaResourceRequest request = peekPendingRequest();
      runContainer(request, ANY_HOST);
    }
  }
}
