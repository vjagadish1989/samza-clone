package org.apache.samza.clustermanager;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by jvenkatr on 3/30/16.
 */
public class TestContainerRequestState {

  private final MockContainerManagerCallback callback = new MockContainerManagerCallback();
  private final MockContainerManager manager = new MockContainerManager(callback);

  private static final String ANY_HOST = ContainerRequestState.ANY_HOST;

  /**
   * Test state after a request is submitted
   */
  @Test
  public void testUpdateRequestState() {
    // Host-affinity is enabled
    ContainerRequestState state = new ContainerRequestState(true, manager);
    SamzaResourceRequest request = new SamzaResourceRequest(1, 1024, "abc", "id0", 0);
    state.addResourceRequest(request);

    assertNotNull(manager.resourceRequests);
    assertEquals(1, manager.resourceRequests.size());

    assertNotNull(state.numPendingRequests() == 1);

    assertNotNull(state.getRequestsToCountMap());
    assertNotNull(state.getRequestsToCountMap().get("abc"));
    assertEquals(1, state.getRequestsToCountMap().get("abc").get());

    assertNotNull(state.getResourcesOnAHost("abc"));
    assertEquals(0, state.getResourcesOnAHost("abc").size());

    // Host-affinity is not enabled
    ContainerRequestState state1 = new ContainerRequestState(false, manager);
    SamzaResourceRequest request1 = new SamzaResourceRequest(1, 1024, null, "id1", 1);
    state1.addResourceRequest(request1);

    assertNotNull(manager.resourceRequests);
    assertEquals(2, manager.resourceRequests.size());


    assertTrue(state1.numPendingRequests() == 1);

    assertNotNull(state1.getRequestsToCountMap());
    assertNull(state1.getRequestsToCountMap().get(ANY_HOST));

  }


  /**
   * Test addContainer() updates the state correctly
   */
  @Test
  public void testAddContainer() {
    // Add container to ANY_LIST when host-affinity is not enabled
    ContainerRequestState state = new ContainerRequestState(false, manager);
    SamzaResource resource = new SamzaResource(1, 1024, "abc", "id1");

    state.addResource(resource);

    assertNotNull(state.getRequestsToCountMap());
    assertNotNull(state.getResourcesOnAHost(ANY_HOST));

    assertEquals(1, state.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(resource, state.getResourcesOnAHost(ANY_HOST).get(0));

    // Container Allocated when there is no request in queue
    ContainerRequestState state1 = new ContainerRequestState(true, manager);
    SamzaResource container1 = new SamzaResource(1, 1024, "zzz", "id2");
    state1.addResource(container1);

    assertEquals(0, state1.numPendingRequests());

    assertNull(state1.getResourcesOnAHost("zzz"));
    assertNotNull(state1.getResourcesOnAHost(ANY_HOST));
    assertEquals(1, state1.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container1, state1.getResourcesOnAHost(ANY_HOST).get(0));

    // Container Allocated on a Requested Host
    state1.addResourceRequest(new SamzaResourceRequest(1, 1024, "abc", "id1", 0));

    assertEquals(1, state1.numPendingRequests());

    assertNotNull(state1.getRequestsToCountMap());
    assertNotNull(state1.getRequestsToCountMap().get("abc"));
    assertEquals(1, state1.getRequestsToCountMap().get("abc").get());

    state1.addResource(resource);

    assertNotNull(state1.getResourcesOnAHost("abc"));
    assertEquals(1, state1.getResourcesOnAHost("abc").size());
    assertEquals(resource, state1.getResourcesOnAHost("abc").get(0));

    // Container Allocated on host that was not requested
    SamzaResource container2 = new SamzaResource(1, 1024, "xyz", "id2");

    state1.addResource(container2);

    assertNull(state1.getResourcesOnAHost("xyz"));
    assertNotNull(state1.getResourcesOnAHost(ANY_HOST));
    assertEquals(2, state1.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container2, state1.getResourcesOnAHost(ANY_HOST).get(1));

    // Extra containers were allocated on a host that was requested
    SamzaResource container3 = new SamzaResource(1, 1024, "abc", "id3");
    state1.addResource(container3);

    assertEquals(3, state1.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container3, state1.getResourcesOnAHost(ANY_HOST).get(2));
  }

  /**
   * Test request state after container is assigned to a host
   * * Assigned on requested host
   * * Assigned on any host
   */
  @Test
  public void testContainerAssignment() throws Exception {
    // Host-affinity enabled
    ContainerRequestState state = new ContainerRequestState(true, manager);
    SamzaResourceRequest request = new SamzaResourceRequest(1, 1024, "abc", "id0", 0);

    SamzaResourceRequest request1 = new SamzaResourceRequest(1, 1024, "def", "id1", 0);

    state.addResourceRequest(request);
    state.addResourceRequest(request1);

    SamzaResource container = new SamzaResource(1, 1024, "abc", "id0");

    SamzaResource container1 = new SamzaResource(1, 1024, "zzz", "id1");
    state.addResource(container);
    state.addResource(container1);

    assertEquals(2, state.numPendingRequests());
    assertEquals(2, state.getRequestsToCountMap().size());

    assertNotNull(state.getResourcesOnAHost("abc"));
    assertEquals(1, state.getResourcesOnAHost("abc").size());
    assertEquals(container, state.getResourcesOnAHost("abc").get(0));

    assertNotNull(state.getResourcesOnAHost("def"));
    assertEquals(0, state.getResourcesOnAHost("def").size());

    assertNotNull(state.getResourcesOnAHost(ANY_HOST));
    assertEquals(1, state.getResourcesOnAHost(ANY_HOST).size());
    assertEquals(container1, state.getResourcesOnAHost(ANY_HOST).get(0));

    // Container assigned on the requested host
    state.updateStateAfterAssignment(request, "abc", container);

    assertEquals(request1, state.peekPendingRequest());

    assertNotNull(state.getRequestsToCountMap().get("abc"));
    assertEquals(0, state.getRequestsToCountMap().get("abc").get());

    assertNotNull(state.getResourcesOnAHost("abc"));
    assertEquals(0, state.getResourcesOnAHost("abc").size());

    // Container assigned on any host
    state.updateStateAfterAssignment(request1, ANY_HOST, container1);

    assertEquals(0, state.numPendingRequests());

    assertNotNull(state.getRequestsToCountMap().get("def"));
    assertEquals(0, state.getRequestsToCountMap().get("def").get());

    assertNotNull(state.getResourcesOnAHost(ANY_HOST));
    assertEquals(0, state.getResourcesOnAHost(ANY_HOST).size());

  }


}
