package org.apache.samza.clustermanager;

import org.apache.samza.job.CommandBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by jvenkatr on 3/28/16.
 */
public class MockContainerManager extends ContainerProcessManager
{
  Set<SamzaResource> releasedResources = new HashSet<>();
  List<SamzaResource> resourceRequests = new ArrayList<>();
  List<SamzaResourceRequest> cancelledRequests = new ArrayList<>();
  List<SamzaResource> launchedResources = new ArrayList<>();
  Throwable nextException = null;

  public MockContainerManager(Callback callback) {
    super(callback);
  }

  @Override
  public void start() {

  }

  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    SamzaResource resource = new SamzaResource(resourceRequest.getNumCores(), resourceRequest.getMemoryMB(), resourceRequest.getPreferredHost(), UUID.randomUUID().toString());
    List<SamzaResource> resources = Collections.singletonList(resource);
    resourceRequests.addAll(resources);
  }

  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {
    cancelledRequests.add(request);
  }

  @Override
  public void releaseResources(SamzaResource resource) {
    releasedResources.add(resource);
  }

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) throws SamzaContainerLaunchException {
    if(nextException != null) {
      throw new SamzaContainerLaunchException(nextException);
    }
    launchedResources.add(resource);
  }

  @Override
  public void stop(SamzaAppState.SamzaAppStatus status) {

  }
}
