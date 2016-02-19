package org.apache.samza.coordinator;

import org.apache.samza.job.CommandBuilder;

import java.util.List;

/**
 * Created by jvenkatr on 2/4/16.
 */
public interface ContainerProcessManager {
    public void start();
    public void requestResources (List<SamzaResourceRequest> resourceRequest, ContainerProcessManagerCallback callback);
    public void releaseResources (List<SamzaResource> resources, ContainerProcessManagerCallback callback);
    public void launchStreamProcessor (SamzaResource resource, int containerID, CommandBuilder builder);
    public void cancelResourceRequest (SamzaResourceRequest request);

    public void stop();
}
