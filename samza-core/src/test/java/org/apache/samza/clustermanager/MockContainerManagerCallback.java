package org.apache.samza.clustermanager;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jvenkatr on 3/28/16.
 */
public class MockContainerManagerCallback implements ContainerProcessManager.Callback {
  List<SamzaResource> resources = new ArrayList<>();
  List<SamzaResourceStatus> resourceStatuses = new ArrayList<>();
  Throwable error;

  @Override
  public void onResourcesAvailable(List<SamzaResource> resourceList) {
    resources.addAll(resourceList);
  }

  @Override
  public void onResourcesCompleted(List<SamzaResourceStatus> resourceStatusList) {
    resourceStatuses.addAll(resourceStatusList);
  }

  @Override
  public void onError(Throwable e) {
    error=e;
  }
}
