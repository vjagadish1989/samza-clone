package org.apache.samza.clustermanager;

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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by jvenkatr on 3/30/16.
 */
public class TestContainerAllocator {
  private final MockContainerManagerCallback callback = new MockContainerManagerCallback();
  private final MockContainerManager manager = new MockContainerManager(callback);
  private final Config config = getConfig();
  private final JobModelReader reader = getJobModelReader(1);
  private final SamzaAppState state = new SamzaAppState(reader);
  private ContainerAllocator containerAllocator;
  private MockContainerRequestState requestState;
  private Thread allocatorThread;

  @Before
  public void setup() throws Exception {
    containerAllocator = new ContainerAllocator(manager, config, state);
    requestState = new MockContainerRequestState(manager, false);
    Field requestStateField = containerAllocator.getClass().getSuperclass().getDeclaredField("resourceRequestState");
    requestStateField.setAccessible(true);
    requestStateField.set(containerAllocator, requestState);
    allocatorThread = new Thread(containerAllocator);
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
        put("yarn.allocator.sleep.ms", "10");
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


  /**
   * Adds all containers returned to ANY_HOST only
   */
  @Test
  public void testAddContainer() throws Exception {
    assertNull(requestState.getResourcesOnAHost("abc"));
    assertNull(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST));

    containerAllocator.addResource(new SamzaResource(1, 1000, "abc", "id1"));
    containerAllocator.addResource(new SamzaResource(1, 1000, "xyz", "id1"));


    assertNull(requestState.getResourcesOnAHost("abc"));
    assertNotNull(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST));
    assertTrue(requestState.getResourcesOnAHost(ContainerRequestState.ANY_HOST).size() == 2);
  }

  /**
   * Test requestContainers
   */
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

    allocatorThread.start();

    containerAllocator.requestResources(containersToHostMapping);

    assertEquals(4, manager.resourceRequests.size());

    assertNotNull(requestState);

    assertEquals(requestState.numPendingRequests(), 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertEquals(requestState.getRequestsToCountMap().keySet().size(), 0);
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

    assertTrue(requestState.numPendingRequests() == 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertTrue(requestState.getRequestsToCountMap().keySet().size() == 0);
  }

  /**
   * Extra allocated containers that are returned by the RM and unused by the AM should be released.
   * Containers are considered "extra" only when there are no more pending requests to fulfill
   * @throws Exception
   */
  @Test
  public void testAllocatorReleasesExtraContainers() throws Exception {
    final SamzaResource resource = new SamzaResource(1,1000,"abc","id1");
    final SamzaResource resource1 = new SamzaResource(1,1000,"abc","id2");
    final SamzaResource resource2 = new SamzaResource(1,1000,"def","id3");


    // Set up our final asserts before starting the allocator thread
    MockContainerListener listener = new MockContainerListener(3, 2, 0, null, new Runnable() {
      @Override
      public void run() {

        assertTrue(manager.releasedResources.contains(resource1));
        assertTrue(manager.releasedResources.contains(resource2));

        // Test that state is cleaned up
        assertEquals(0, requestState.numPendingRequests());
        assertEquals(0, requestState.getRequestsToCountMap().size());
        assertNull(requestState.getResourcesOnAHost("abc"));
        assertNull(requestState.getResourcesOnAHost("def"));
      }
    }, null);
    requestState.registerContainerListener(listener);

    allocatorThread.start();

    containerAllocator.requestResource(0, "abc");

    containerAllocator.addResource(resource);
    containerAllocator.addResource(resource1);
    containerAllocator.addResource(resource2);

    listener.verify();
  }


}
