package org.apache.samza.coordinator;

import java.util.List;

/**
 * Created by jvenkatr on 2/4/16.
 */
public interface ContainerProcessManagerCallback {
    public void onResourcesAvailable (List<SamzaResource> resources);
    public void onResourcesWithdrawn (List<SamzaResource> resources);
    public void onResourcesCompleted (List<SamzaResourceStatus> resources);
    public void onError (Exception e);

}
