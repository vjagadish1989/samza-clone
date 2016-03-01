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
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Samza's application master is mostly interested in requesting containers to
 * run Samza jobs. SamzaTaskManager is responsible for requesting new
 * containers, handling failures, and notifying the application master that the
 * job is done.
 *
 * The following main threads are involved in the execution of the Samza AM:
 *  - The main thread (defined in SamzaAppMaster) that drive the AM to send out container requests to RM
 *  - The callback handler thread that receives the responses from RM and handles:
 *      - Populating a buffer when a container is allocated by the RM
 *        (allocatedContainers in {@link org.apache.samza.clustermanager.ContainerRequestState}
 *      - Identifying the cause of container failure & re-request containers from RM by adding request to the
 *        internal requestQueue in {@link org.apache.samza.clustermanager.ContainerRequestState}
 *  - The allocator thread defined here assigns the allocated containers to pending requests
 *    (See {@link org.apache.samza.clustermanager.ContainerAllocator} or {@link org.apache.samza.clustermanager.HostAwareContainerAllocator})
 */

public class SamzaTaskManager implements JobCoordinatorListener {
    private static final Logger log = LoggerFactory.getLogger(SamzaTaskManager.class);

    private final boolean hostAffinityEnabled;
    private final SamzaAppState state;


    // Derived configs
    private final JobConfig jobConfig;

    private final AbstractContainerAllocator containerAllocator;
    private final Thread allocatorThread;

    private  ContainerProcessManager manager = null;

    // State
    private boolean tooManyFailedContainers = false;
    private Map<Integer, ResourceFailure> containerFailures = new HashMap<Integer, ResourceFailure>();

    public SamzaTaskManager(Config config,
                            SamzaAppState state,
                            ContainerProcessManager manager
                            ) {
        log.info("initialized samza task manager");
        this.state = state;
        this.jobConfig = new JobConfig(config);
        this.manager=manager;

        this.hostAffinityEnabled = jobConfig.getHostAffinityEnabled();

        if (this.hostAffinityEnabled) {
            this.containerAllocator = new HostAwareContainerAllocator(
                    manager,
                    jobConfig.getContainerRequestTimeout(),
                    config,
                    state
            );
        } else {
            this.containerAllocator = new ContainerAllocator(
                    manager,
                    config,
                    state);
        }

        this.allocatorThread = new Thread(this.containerAllocator, "Container Allocator Thread");
        log.info("finished initialization of samza task manager");

    }

    public boolean shouldShutdown() {
        log.info(" check task manager" + tooManyFailedContainers + "" +state.completedContainers.get() + " " + state.containerCount + " " + allocatorThread.isAlive() );
        return tooManyFailedContainers || state.completedContainers.get() == state.containerCount || !allocatorThread.isAlive();
    }

    public void onInit() {
        log.info("started on Init of  samza task manager");

        state.containerCount = jobConfig.getContainerCount();

        state.neededContainers.set(state.containerCount);

        // Request initial set of containers
        Map<Integer, String> containerToHostMapping = state.jobCoordinator.jobModel().getAllContainerLocality();

        containerAllocator.requestContainers(containerToHostMapping);

        // Start container allocator thread
        log.info("Starting the container allocator thread");
        allocatorThread.start();
        log.info("finished on Init of  samza task manager");

    }

    public void onReboot() {

    }

    public void onShutdown() {
        log.info("Called onShutdown of Samza task manager");

        // Shutdown allocator thread
        containerAllocator.setIsRunning(false) ;
        try {
            allocatorThread.join();
        } catch (InterruptedException ie) {
            log.info("Allocator Thread join() threw an interrupted exception", ie);
            // Should we throw exception here??
        }
        log.info("finished onShutdown of Samza task manager");

    }

    public void onContainerAllocated(SamzaResource container) {
        log.info("Container allocated from RM on " + container.getHost());
        containerAllocator.addContainer(container);
    }

    /**
     * This methods handles the onContainerCompleted callback from the RM. Based on the ContainerExitStatus, it decides
     * whether a container that exited is marked as complete or failure.
     */
    public void onContainerCompleted(SamzaResourceStatus containerStatus) {
        String containerIdStr = containerStatus.resourceID;
        int containerId = -1;
        for(Map.Entry<Integer, SamzaResource> entry: state.runningContainers.entrySet()) {
            if(entry.getValue().getResourceID().equals(containerStatus.getResourceID())) {
                log.info("Matching container ID found " + entry.getKey() + " " + entry.getValue() );

                containerId = entry.getKey();
                break;
            }
        }
        if(containerId == -1) {
          log.info("No matching container id found for " + containerStatus.toString());
        }
        state.runningContainers.remove(containerId);

        int exitStatus = containerStatus.getExitCode();
        switch(exitStatus) {
            case SamzaResourceStatus.SUCCESS:
                log.info("Container {} completed successfully.", containerIdStr);

                state.completedContainers.incrementAndGet();

                if (containerId != -1) {
                    state.finishedContainers.add(containerId);
                    containerFailures.remove(containerId);
                }

                if (state.completedContainers.get() == state.containerCount) {
                    log.info("Setting job status to SUCCEEDED, since all containers have been marked as completed.");
                    state.status = "succeeded";
                }
                break;

            case SamzaResourceStatus.DISK_FAIL:
            case SamzaResourceStatus.ABORTED:
            case SamzaResourceStatus.PREEMPTED:
                log.info("Got an exit code of {}. This means that container {} was "
                                + "killed by YARN, either due to being released by the application "
                                + "master or being 'lost' due to node failures etc. or due to preemption by the RM",
                        exitStatus,
                        containerIdStr);

                state.releasedContainers.incrementAndGet();

                // If this container was assigned some partitions (a containerId), then
                // clean up, and request a new container for the tasks. This only
                // should happen if the container was 'lost' due to node failure, not
                // if the AM released the container.
                if (containerId != -1) {
                    log.info("Released container {} was assigned task group ID {}. Requesting a new container for the task group.", containerIdStr, containerId);

                    state.neededContainers.incrementAndGet();
                    state.jobHealthy.set(false);

                    // request a container on new host
                    containerAllocator.requestContainer(containerId, ContainerAllocator.ANY_HOST);
                }
                break;

            default:
                // TODO: Handle failure more intelligently. Should track NodeFailures!
                log.info("Container failed for some reason. Let's start it again");
                log.info("Container " + containerIdStr + " failed with exit code . " + exitStatus + " - " + containerStatus.getDiagnostics() + " containerID is " + containerId);

                state.failedContainers.incrementAndGet();
                state.failedContainersStatus.put(containerIdStr, containerStatus);
                state.jobHealthy.set(false);

                if(containerId != -1) {
                    state.neededContainers.incrementAndGet();
                    // Find out previously running container location
                    String lastSeenOn = state.jobCoordinator.jobModel().getContainerToHostValue(containerId, SetContainerHostMapping.HOST_KEY);
                    if (!hostAffinityEnabled || lastSeenOn == null) {
                        lastSeenOn = ContainerAllocator.ANY_HOST;
                    }
                    log.info("Container was last seen on " + lastSeenOn );
                    // A container failed for an unknown reason. Let's check to see if
                    // we need to shutdown the whole app master if too many container
                    // failures have happened. The rules for failing are that the
                    // failure count for a task group id must be > the configured retry
                    // count, and the last failure (the one prior to this one) must have
                    // happened less than retry window ms ago. If retry count is set to
                    // 0, the app master will fail on any container failure. If the
                    // retry count is set to a number < 0, a container failure will
                    // never trigger an app master failure.
                    int retryCount = jobConfig.getContainerRetryCount();
                    int retryWindowMs = jobConfig.getContainerRetryWindowMs();

                    if (retryCount == 0) {
                        log.error("Container ID {} ({}) failed, and retry count is set to 0, so shutting down the application master, and marking the job as failed.", containerId, containerIdStr);

                        tooManyFailedContainers = true;
                    } else if (retryCount > 0) {
                        int currentFailCount;
                        long lastFailureTime;
                        if(containerFailures.containsKey(containerId)) {
                            ResourceFailure failure = containerFailures.get(containerId);
                            currentFailCount = failure.getCount() + 1;
                            lastFailureTime = failure.getLastFailure();
                        } else {
                            currentFailCount = 1;
                            lastFailureTime = 0L;
                        }
                        if (currentFailCount >= retryCount) {
                            long lastFailureMsDiff = System.currentTimeMillis() - lastFailureTime;

                            if (lastFailureMsDiff < retryWindowMs) {
                                log.error("Container ID " + containerId + "(" + containerIdStr + ") has failed " + currentFailCount +
                                        " times, with last failure " + lastFailureMsDiff + "ms ago. This is greater than retry count of " +
                                        retryCount + " and window of " + retryWindowMs + "ms , so shutting down the application master, and marking the job as failed.");

                                // We have too many failures, and we're within the window
                                // boundary, so reset shut down the app master.
                                tooManyFailedContainers = true;
                                state.status = "failed";
                            } else {
                                log.info("Resetting fail count for container ID {} back to 1, since last container failure ({}) for " +
                                        "this container ID was outside the bounds of the retry window.", containerId, containerIdStr);

                                // Reset counter back to 1, since the last failure for this
                                // container happened outside the window boundary.
                                containerFailures.put(containerId, new ResourceFailure(1, System.currentTimeMillis()));
                            }
                        } else {
                            log.info("Current fail count for container ID {} is {}.", containerId, currentFailCount);
                            containerFailures.put(containerId, new ResourceFailure(currentFailCount, System.currentTimeMillis()));
                        }
                    }

                    if (!tooManyFailedContainers) {
                        log.info("Requesting a new container ");
                        // Request a new container
                        containerAllocator.requestContainer(containerId, lastSeenOn);
                    }
                }

        }
    }

}
