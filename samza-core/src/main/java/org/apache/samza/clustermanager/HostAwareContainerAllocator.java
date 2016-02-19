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

/**
 * This is the allocator thread that will be used by SamzaTaskManager when host-affinity is enabled for a job. It is similar to {@link ContainerAllocator}, except that it considers container locality for allocation.
 *
 * In case of host-affinity, each container request ({@link SamzaResourceRequest} encapsulates the identifier of the container to be run and a "preferredHost". preferredHost is determined by the locality mappings in the coordinator stream.
 * This thread periodically wakes up and makes the best-effort to assign a container to the preferredHost. If the preferredHost is not returned by the RM before the corresponding container expires, the thread assigns the container to any other host that is allocated next.
 * The container expiry is determined by CONTAINER_REQUEST_TIMEOUT and is configurable on a per-job basis.
 *
 * If there aren't enough containers, it waits by sleeping for ALLOCATOR_SLEEP_TIME milliseconds.
 */
public class HostAwareContainerAllocator extends AbstractContainerAllocator {
  private static final Logger log = LoggerFactory.getLogger(HostAwareContainerAllocator.class);

  private final int CONTAINER_REQUEST_TIMEOUT;

  public HostAwareContainerAllocator(ContainerProcessManager manager ,
                                     int timeout, Config config, SamzaAppState state) {
    super(manager, new ContainerRequestState(true, manager), config, state);
    this.CONTAINER_REQUEST_TIMEOUT = timeout;
  }

  /**
   * Since host-affinity is enabled, all allocated container resources are buffered in the list keyed by "preferredHost".
   *
   * If the requested host is not available, the thread checks to see if the request has expired.
   * If it has expired, it runs the container with expectedContainerID on one of the available hosts from the
   * allocatedContainers buffer keyed by "ANY_HOST".
   */
  @Override
  public void run() {
    try {
      while (isRunning.get()) {
        while (!containerRequestState.getRequestsQueue().isEmpty()) {
          SamzaResourceRequest request = containerRequestState.getRequestsQueue().peek();
          String preferredHost = request.getPreferredHost();
          int expectedContainerId = request.expectedContainerID;
            CommandBuilder builder = getCommandBuilder(expectedContainerId);


            log.info(
              "Handling request for container id {} on preferred host {}",
              expectedContainerId,
              preferredHost);

          List<SamzaResource> allocatedContainers = containerRequestState.getContainersOnAHost(preferredHost);
          if (allocatedContainers != null && allocatedContainers.size() > 0) {
            // Found allocated container at preferredHost
            SamzaResource container = allocatedContainers.get(0);

            containerRequestState.updateStateAfterAssignment(request, preferredHost, container);

            log.info("Running {} on {}", expectedContainerId, container.getResourceID());

            //TODO; add builder niceties
            amClient.launchStreamProcessor(container, expectedContainerId, builder);
            state.matchedContainerRequests.incrementAndGet();

            //containerUtil.runMatchedContainer(expectedContainerId, container);
          } else {
            // No allocated container on preferredHost
            log.info(
                "Did not find any allocated containers on preferred host {} for running container id {}",
                preferredHost,
                expectedContainerId);
            boolean expired = requestExpired(request);
            allocatedContainers = containerRequestState.getContainersOnAHost(ANY_HOST);
            if (!expired || allocatedContainers == null || allocatedContainers.size() == 0) {
              log.info("Either the request timestamp {} is greater than container request timeout {}ms or we couldn't " +
                      "find any free allocated containers in the buffer. Breaking out of loop.",
                  request.getRequestTimestamp(),
                  CONTAINER_REQUEST_TIMEOUT);
              break;
            } else {
              if (allocatedContainers.size() > 0) {
                SamzaResource container = allocatedContainers.get(0);
                log.info("Found available containers on ANY_HOST. Assigning request for container_id {} with " +
                        "timestamp {} to container {}",
                    new Object[] { String.valueOf(expectedContainerId), request.getRequestTimestamp(), container.getResourceID()
                });
                containerRequestState.updateStateAfterAssignment(request, ANY_HOST, container);
                log.info("Running {} on {}", expectedContainerId, container.getResourceID());
                //TODO: builder niceities
                amClient.launchStreamProcessor(container, expectedContainerId, builder);

                //containerUtil.runContainer(expectedContainerId, container);
              }
            }
          }
        }
        // Release extra containers and update the entire system's state
        containerRequestState.releaseExtraContainers();

        Thread.sleep(ALLOCATOR_SLEEP_TIME);
      }
    } catch (InterruptedException ie) {
      log.info("Got an InterruptedException in HostAwareContainerAllocator thread!", ie);
    } catch (Exception e) {
      log.info("Got an unknown Exception in HostAwareContainerAllocator thread!", e);
    }
  }

  private boolean requestExpired(SamzaResourceRequest request) {
    return System.currentTimeMillis() - request.requestTimestamp > CONTAINER_REQUEST_TIMEOUT;
  }
}
