 /* Licensed to the Apache Software Foundation (ASF) under one
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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobModelReader;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestHostAwareContainerAllocator {

  private final MockContainerManagerCallback callback = new MockContainerManagerCallback();
  private final MockContainerManager manager = new MockContainerManager(callback);
  private final Config config = getConfig();
  private final JobModelReader reader = getJobModelReader(1);
  private final SamzaAppState state = new SamzaAppState(reader);
  private HostAwareContainerAllocator containerAllocator;
  private final int TIMEOUT_MS = 1000;
  private MockContainerRequestState requestState;
  private Thread allocatorThread;

  @Before
  public void setup() throws Exception {
    containerAllocator = new HostAwareContainerAllocator(manager, TIMEOUT_MS, config, state);
    requestState = new MockContainerRequestState(manager, true);
    Field requestStateField = containerAllocator.getClass().getSuperclass().getDeclaredField("resourceRequestState");
    requestStateField.setAccessible(true);
    requestStateField.set(containerAllocator, requestState);
    allocatorThread = new Thread(containerAllocator);
  }


  /**
   * Test request containers with no containerToHostMapping makes the right number of requests
   */
  @Test
  public void testRequestContainersWithNoMapping() throws Exception {
    int containerCount = 4;
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>();
    for (int i = 0; i < containerCount; i++) {
      containersToHostMapping.put(i, null);
    }

    allocatorThread.start();

    containerAllocator.requestResources(containersToHostMapping);

    assertNotNull(requestState);

    assertEquals(4, requestState.numPendingRequests());

    assertNotNull(requestState.getRequestsToCountMap());
    assertEquals(1, requestState.getRequestsToCountMap().keySet().size());
    assertTrue(requestState.getRequestsToCountMap().keySet().contains(ContainerRequestState.ANY_HOST));
  }

  /**
   * Add containers to the correct host in the request state
   */
  @Test
  public void testAddContainerWithHostAffinity() throws Exception {
    containerAllocator.requestResources(new HashMap<Integer, String>() {
      {
        put(0, "abc");
        put(1, "xyz");
      }
    });

    assertNotNull(requestState.getResourcesOnAHost("abc"));
    assertEquals(0, requestState.getResourcesOnAHost("abc").size());

    assertNotNull(requestState.getResourcesOnAHost("xyz"));
    assertEquals(0, requestState.getResourcesOnAHost("xyz").size());

    assertNull(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST));

    containerAllocator.addResource(new SamzaResource(1, 10, "abc", "ID1"));
    containerAllocator.addResource(new SamzaResource(1, 10, "def", "ID2"));
    containerAllocator.addResource(new SamzaResource(1, 10, "xyz", "ID3"));


    assertNotNull(requestState.getResourcesOnAHost("abc"));
    assertEquals(1, requestState.getResourcesOnAHost("abc").size());

    assertNotNull(requestState.getResourcesOnAHost("xyz"));
    assertEquals(1, requestState.getResourcesOnAHost("xyz").size());

    assertNotNull(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST));
    assertTrue(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST).size() == 1);
    assertEquals("ID2", requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST).get(0).getResourceID());
  }

  @Test
  public void testAllocatorReleasesExtraContainers() throws Exception {
    final SamzaResource resource0 = new SamzaResource(1, 1024, "abc", "id1");
    final SamzaResource resource1 = new SamzaResource(1, 1024, "abc", "id2");
    final SamzaResource resource2 = new SamzaResource(1, 1024, "def", "id3");

    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(3, 2, 0, null, new Runnable() {
      @Override
      public void run() {
        assertEquals(2, manager.releasedResources.size());
        assertTrue(manager.releasedResources.contains(resource1));
        assertTrue(manager.releasedResources.contains(resource2));

        // Test that state is cleaned up
        assertEquals(0, requestState.numPendingRequests());
        assertEquals(0, requestState.getRequestsToCountMap().size());
        assertNull(requestState.getResourcesOnAHost("abc"));
        assertNull(requestState.getResourcesOnAHost("def"));
      }
    },
        null);
    requestState.registerContainerListener(listener);

    allocatorThread.start();

    containerAllocator.requestResource(0, "abc");

    containerAllocator.addResource(resource0);
    containerAllocator.addResource(resource1);
    containerAllocator.addResource(resource2);

    listener.verify();
  }




  @Test
  public void testRequestContainers() throws Exception {
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>() {
      {
        put(0, "abc");
        put(1, "def");
        put(2, null);
        put(3, "abc");
      }
    };

    containerAllocator.requestResources(containersToHostMapping);

    assertNotNull(manager.resourceRequests);
    assertEquals(manager.resourceRequests.size(), 4);
    assertEquals(requestState.numPendingRequests(), 4);

    Map<String, AtomicInteger> requestsMap = requestState.getRequestsToCountMap();
    assertNotNull(requestsMap.get("abc"));
    assertEquals(2, requestsMap.get("abc").get());

    assertNotNull(requestsMap.get("def"));
    assertEquals(1, requestsMap.get("def").get());

    assertNotNull(requestsMap.get(ContainerRequestState.ANY_HOST));
    assertEquals(1, requestsMap.get(ContainerRequestState.ANY_HOST).get());
  }

  /**
   * Handles expired requests correctly and assigns ANY_HOST
   */

  @Test
  public void testExpiredRequestHandling() throws Exception {
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>() {
      {
        put(0, "abc");
        put(1, "def");
      }
    };
    containerAllocator.requestResources(containersToHostMapping);
    assertEquals(requestState.numPendingRequests(), 2);
    assertNotNull(requestState.getRequestsToCountMap());
    assertNotNull(requestState.getRequestsToCountMap().get("abc"));
    assertTrue(requestState.getRequestsToCountMap().get("abc").get() == 1);

    assertNotNull(requestState.getRequestsToCountMap().get("def"));
    assertTrue(requestState.getRequestsToCountMap().get("def").get() == 1);

    MockContainerListener listener = new MockContainerListener(2, 0, 0, new Runnable() {
      @Override
      public void run() {
        assertNull(requestState.getResourcesOnAHost("xyz"));
        assertNull(requestState.getResourcesOnAHost("zzz"));
        assertNotNull(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST));
        assertTrue(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST).size() == 2);
      }
    }, null, null);
    requestState.registerContainerListener(listener);

    allocatorThread.start();

    SamzaResource resource0 = new SamzaResource(1,1000,"xyz","id1");
    SamzaResource resource1 = new SamzaResource(1,1000,"zzz","id2");
    containerAllocator.addResource(resource0);
    containerAllocator.addResource(resource1);
    listener.verify();

    Thread.sleep(2000);
    System.out.println(manager.launchedResources);
    assertTrue(manager.launchedResources.contains(resource0));
    assertTrue(manager.launchedResources.contains(resource1));

    assertEquals(requestState.numPendingRequests(), 0);
    assertNotNull(requestState.getRequestsToCountMap());
    assertNull(requestState.getRequestsToCountMap().get("abc"));
    assertNull(requestState.getRequestsToCountMap().get("def"));

  }

  @After
  public void teardown() throws Exception {
    reader.stop();
    containerAllocator.stop();
  }


  private static Config getConfig() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        put("yarn.container.count", "1");
        put("systems.test-system.samza.factory", "org.apache.samza.job.yarn.MockSystemFactory");
        put("yarn.container.memory.mb", "512");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
        put("yarn.container.retry.count", "1");
        put("yarn.container.retry.window.ms", "1999999999");
        put("yarn.samza.host-affinity.enabled", "true");
        put("yarn.container.request.timeout.ms", "3");
        put("yarn.allocator.sleep.ms", "1");
      }
    });

    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

  private static JobModelReader getJobModelReader(int containerCount) {
    //Ideally, the JobModelReader should be constructed independent of HttpServer.
    //That way it becomes easier to mock objects. Save it for later.

    HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
    Map<Integer, ContainerModel> containers = new java.util.HashMap<>();
    for (int i = 0; i < containerCount; i++) {
      ContainerModel container = new ContainerModel(i, new HashMap<TaskName, TaskModel>());
      containers.put(i, container);
    }
    JobModel jobModel = new JobModel(getConfig(), containers);
    return new JobModelReader(jobModel, server);
  }


}
