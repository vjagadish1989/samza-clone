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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ContainerRequestState} maintains the state variables for all the container requests and the allocated containers returned
 * by the RM.
 *
 * This class is thread-safe, and can safely support concurrent accesses without any form of external synchronization. Currently,
 * this state is shared across both the Allocator Thread, and the Callback handler thread.
 *
 */
public class ContainerRequestState {
  private static final Logger log = LoggerFactory.getLogger(ContainerRequestState.class);
  public static final String ANY_HOST = "ANY_HOST";

  /**
   * Maintain a map of hostname to a list of containers allocated on this host
   */
  private final Map<String, List<SamzaResource>> allocatedContainers = new HashMap<>();
  /**
   * Represents the queue of container requests made by the {@link org.apache.samza.clustermanager.SamzaTaskManager}
   */
  private final PriorityQueue<SamzaResourceRequest> requestsQueue = new PriorityQueue<SamzaResourceRequest>();
  /**
   * Maintain a map of hostname to the number of requests made for containers on this host
   * This state variable is used to look-up whether an allocated container on a host was ever requested in the past.
   * This map is not updated when host-affinity is not enabled
   */
  private final Map<String, AtomicInteger> requestsToCountMap = new HashMap<>();
  /**
   * Indicates whether host-affinity is enabled or not
   */
  private final boolean hostAffinityEnabled;

  private final ContainerProcessManager manager;

  private final Object lock = new Object();

  public ContainerRequestState(boolean hostAffinityEnabled, ContainerProcessManager manager) {
    this.hostAffinityEnabled = hostAffinityEnabled;
    this.manager = manager;
  }

  /**
   * Enqueues a {@link SamzaResourceRequest} to be sent to a {@link ContainerProcessManager}.
   *
   * @param request {@link SamzaResourceRequest} to be queued
   */
  public void addResourceRequest(SamzaResourceRequest request) {
    synchronized (lock) {
      requestsQueue.add(request);
      String preferredHost = request.getPreferredHost();

      // if host affinity is enabled, update state.
      if (hostAffinityEnabled) {
        //increment # of requests on the host.
        if (requestsToCountMap.containsKey(preferredHost)) {
          requestsToCountMap.get(preferredHost).incrementAndGet();
        } else {
          requestsToCountMap.put(preferredHost, new AtomicInteger(1));
        }
        /**
         * The following is important to correlate allocated container data with the requestsQueue made before. If
         * the preferredHost is requested for the first time, the state should reflect that the allocatedContainers
         * list is empty and NOT null.
         */

        if (!allocatedContainers.containsKey(preferredHost)) {
          allocatedContainers.put(preferredHost, new ArrayList<SamzaResource>());
        }
      }
      manager.requestResources(request);
    }
  }

  /**
   * Invoked each time a resource is returned from a {@link ContainerProcessManager}.
   * @param container The resource that was returned from the {@link ContainerProcessManager}
   */
  public void addResource(SamzaResource container) {
    synchronized (lock) {
      if (hostAffinityEnabled) {
        String hostName = container.getHost();
        AtomicInteger requestCount = requestsToCountMap.get(hostName);
        // Check if this host was requested for any of the containers
        if (requestCount == null || requestCount.get() == 0) {
          log.info(
              " This host was not requested. {} saving the container {} in the buffer for ANY_HOST",
              hostName,
              container.getResourceID()
          );
          addToAllocatedContainerList(ANY_HOST, container);
        } else {
          // This host was indeed requested.
          int requestCountOnThisHost = requestCount.get();
          List<SamzaResource> allocatedContainersOnThisHost = allocatedContainers.get(hostName);
          if (requestCountOnThisHost > 0) {
            //there are pending requests for containers on this host.
            if (allocatedContainersOnThisHost == null || allocatedContainersOnThisHost.size() < requestCountOnThisHost) {
              log.info("Got matched container {} in the buffer for preferredHost: {}", container.getResourceID(), hostName);
              addToAllocatedContainerList(hostName, container);
            } else {
              /**
               * The RM may allocate more containers on a given host than requested. In such a case, even though the
               * requestCount != 0, it will be greater than the total request count for that host. Hence, it should be
               * assigned to ANY_HOST
               */
              log.info(
                  "The number of containers already allocated on {} is greater than what was " +
                      "requested, which is {}. Hence, saving the container {} in the buffer for ANY_HOST",
                  new Object[]{
                      hostName,
                      requestCountOnThisHost,
                      container.getResourceID()
                  }
              );
              addToAllocatedContainerList(ANY_HOST, container);
            }
          }
        }
      } else {
        log.info("Host affinity not enabled. Saving the container {} in the buffer for ANY_HOST", container.getResourceID());
        addToAllocatedContainerList(ANY_HOST, container);
      }
    }
  }

  // Appends a container to the list of allocated containers
  private void addToAllocatedContainerList(String host, SamzaResource container) {
    List<SamzaResource> containers = allocatedContainers.get(host);
    if (containers != null) {
      containers.add(container);
    } else {
      containers = new ArrayList<SamzaResource>();
      containers.add(container);
      allocatedContainers.put(host, containers);
    }
  }

  /**
   * This method updates the state after a request is fulfilled and a container starts running on a host
   * Needs to be synchronized because the state buffers are populated by the AMRMCallbackHandler, whereas it is
   * drained by the allocator thread
   *
   * @param request {@link SamzaResourceRequest} that was fulfilled
   * @param assignedHost  Host to which the container was assigned
   * @param container Allocated container resource that was used to satisfy this request
   */
  public void updateStateAfterAssignment(SamzaResourceRequest request, String assignedHost, SamzaResource container) {
    synchronized (lock) {
      requestsQueue.remove(request);
      allocatedContainers.get(assignedHost).remove(container);
      if (hostAffinityEnabled) {
        // assignedHost may not always be the preferred host.
        // Hence, we should safely decrement the counter for the preferredHost
        requestsToCountMap.get(request.getPreferredHost()).decrementAndGet();
      }
      // To avoid getting back excess containers
      manager.cancelResourceRequest(request);
    }
  }

  /**
   * If requestQueue is empty, all extra containers in the buffer should be released and update the entire system's state
   * Needs to be synchronized because it is modifying shared state buffers
   * @return the number of containers released.
   */
  public int releaseExtraResources() {
    synchronized (lock) {
      int numReleasedContainers = 0;
      if (requestsQueue.isEmpty()) {
        log.debug("Container Requests Queue is empty.");
        if (hostAffinityEnabled) {
          List<String> allocatedHosts = getAllocatedHosts();
          for (String host : allocatedHosts) {
            numReleasedContainers += releaseContainersForHost(host);
          }
        } else {
          numReleasedContainers += releaseContainersForHost(ANY_HOST);
        }
        clearState();
      }
      return numReleasedContainers;
    }
  }
  /**
   * Releases all allocated containers for the specified host.
   * @param host  the host for which the containers should be released.
   * @return      the number of containers released.
   */
  private int releaseContainersForHost(String host) {
    int numReleasedContainers = 0;
    List<SamzaResource> containers = allocatedContainers.get(host);
    if (containers != null) {
      for (SamzaResource container : containers) {
        log.info("Releasing extra container {} allocated on {}", container.getResourceID(), host);
        manager.releaseResources(container);
        numReleasedContainers++;
      }
    }
    return numReleasedContainers;
  }


  /**
   * Clears all the state variables
   * Performed when there are no more unfulfilled requests
   */
  private void clearState() {
    allocatedContainers.clear();
    requestsToCountMap.clear();
    requestsQueue.clear();
  }

  /**
   * Returns the list of hosts which has at least 1 allocatedContainer in the buffer
   * @return list of host names
   */
  private List<String> getAllocatedHosts() {
    List<String> hostKeys = new ArrayList<String>();
    for(Map.Entry<String, List<SamzaResource>> entry: allocatedContainers.entrySet()) {
      if(entry.getValue().size() > 0) {
        hostKeys.add(entry.getKey());
      }
    }
    return hostKeys;
  }

  /**
   * Retrieves, but does not remove, the first allocated container on the specified host.
   *
   * @param host  the host for which a container is needed.
   * @return      the first {@link SamzaResource} allocated for the specified host or {@code null} if there isn't one.
   */

  public SamzaResource peekContainer(String host)
  {
    synchronized (lock) {
      List<SamzaResource> containersOnTheHost = this.allocatedContainers.get(host);

      if (containersOnTheHost == null || containersOnTheHost.isEmpty()) {
        return null;
      }
      return containersOnTheHost.get(0);
    }
  }

  /**
   * Retrieves, but does not remove, the next pending request in the queue.
   *
   * @return  the pending request or {@code null} if there is no pending request.
   */
  public SamzaResourceRequest peekPendingRequest() {
    synchronized (lock) {
      return this.requestsQueue.peek();
    }
  }

  /**
   * Returns the number of pending SamzaContainer requests in the queue.
   */
  public int numPendingRequests() {
    synchronized (lock) {
      return this.requestsQueue.size();
    }
  }


  /**
   * Returns the list of containers allocated on a given host. If no containers were ever allocated on
   * the given host, it returns null. This method makes a defensive shallow copy. A shallow copy is
   * sufficient because the SamzaResource class does not expose setters.
   *
   * @param host hostname
   * @return list of containers allocated on the given host, or null
   */
  public List<SamzaResource> getContainersOnAHost(String host) {
    synchronized (lock) {
      List<SamzaResource> containerList = allocatedContainers.get(host);
      if (containerList == null)
        return null;

      return new ArrayList<SamzaResource>(containerList);
    }
  }

}
