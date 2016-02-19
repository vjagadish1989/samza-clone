package org.apache.samza.clustermanager;

/**
 * Created by jvenkatr on 2/15/16.
 */
public interface JobCoordinatorListener {
    public boolean shouldShutdown();
    public void onInit();
    public void onReboot();
    public void onShutdown();
    public void onContainerAllocated(SamzaResource resource);
    public void onContainerCompleted(SamzaResourceStatus resource);
}
