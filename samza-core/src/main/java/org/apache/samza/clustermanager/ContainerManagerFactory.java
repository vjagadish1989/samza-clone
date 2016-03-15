package org.apache.samza.clustermanager;

import org.apache.samza.coordinator.JobModelReader;

/**
 * A factory to build a {@link ContainerProcessManager}
 * //TODO: move the class to Samza-API?
 */
public interface ContainerManagerFactory
{
  /**
   * Return a ContainerProcessManager
   * @param reader to read the {@link org.apache.samza.job.model.JobModel} from
   * @param callback to be registered with the {@link ContainerProcessManager}
   * @param state Useful if the ContainerProcessManager wants to host an UI.
   * //TODO: Remove the SamzaAppState param and refactor into a smaller focussed class.
   * //TODO: Investigate the possibility a common Samza UI for all cluster managers - Yarn,Mesos,Standalone
   * @return
   */
  public ContainerProcessManager getContainerProcessManager(JobModelReader reader, ContainerProcessManager.Callback callback, SamzaAppState state);
}
