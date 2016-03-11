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
import scala.xml.dtd.ANY;

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
  public void assignContainerRequests()  {
    while (hasPendingRequest()) {

          SamzaResourceRequest request = peekPendingRequest();;
          log.info("Handling request: " + request.expectedContainerID + " " + request.requestTimestamp + " " + request.preferredHost);
          String preferredHost = request.getPreferredHost();
          int expectedContainerId = request.expectedContainerID;

          if (hasAllocatedContainer(preferredHost)) {
            // Found allocated container at preferredHost
            log.info("Found_a_matched_container container on the preferred host. Running on" +  expectedContainerId + " " + request.getExpectedContainerID() + " " + preferredHost);
            runContainer(request, preferredHost);
            state.matchedContainerRequests.incrementAndGet();

          } else {
            log.info("Did not find any allocated containers on preferred host {} for running container id {}",
                preferredHost, expectedContainerId);

            boolean expired = requestExpired(request);
            boolean containerAvailableOnAnyHost = hasAllocatedContainer(ANY_HOST);

            if(expired && containerAvailableOnAnyHost) {
              log.info("Request expired. running on ANY_HOST");
              runContainer(request, ANY_HOST);
            }
            else {
              log.info("Either the request timestamp {} is greater than container request timeout {}ms or we couldn't "
                      + "find any free allocated containers in the buffer. Breaking out of loop.",
                  request.getRequestTimestamp(), CONTAINER_REQUEST_TIMEOUT);
              break;
            }
          }
        }
  }

  private boolean requestExpired(SamzaResourceRequest request) {
    long currTime = System.currentTimeMillis();
    boolean requestExpired =  currTime - request.requestTimestamp > CONTAINER_REQUEST_TIMEOUT;
    if(requestExpired == true) {
      log.info("Request " + request + " with currTime " + currTime + " has expired");
    }

      return requestExpired;
  }
}
