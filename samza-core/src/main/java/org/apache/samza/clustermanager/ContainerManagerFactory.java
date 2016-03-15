package org.apache.samza.clustermanager;

import org.apache.samza.coordinator.JobModelReader;

/**
 * A factory to build a {@link ContainerProcessManager}
 */
public interface ContainerManagerFactory
{
  /**
   * Return a ContainerProcessManager
   * @param reader to read the {@link org.apache.samza.job.model.JobModel} from
   * @param callback to be registered with the {@link ContainerProcessManager}
   * @return
   */
  public ContainerProcessManager getContainerProcessManager(JobModelReader reader, ContainerProcessManager.Callback callback);
}
