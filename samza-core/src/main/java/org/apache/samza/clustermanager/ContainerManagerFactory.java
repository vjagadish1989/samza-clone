package org.apache.samza.clustermanager;

import org.apache.samza.coordinator.JobModelReader;

/**
 * Created by jvenkatr on 3/8/16.
 */
public interface ContainerManagerFactory
{
    public ContainerProcessManager getContainerProcessManager(JobModelReader reader, ContainerProcessManager.Callback callback);
}
