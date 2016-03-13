package org.apache.samza.clustermanager;

import org.apache.samza.job.CommandBuilder;

import java.util.List;

/**
 * <code>ContainerProcessManager</code> handles communication with a cluster manager
 * and provides updates on events such as resource allocations and
 * completions. Any offer-based resource management system that integrates with Samza
 * will provide an implementation of a ContainerProcessManager API.
 *
 * This class is meant to be used by implementing a CallbackHandler:
 * <pre>
 * {@code
 * class MyCallbackHandler implements ContainerProcessManager.CallbackHandler {
 *   public void onResourcesAvailable(List<SamzaResource> resources) {
 *     [launch a streamprocessor on the resources]
 *   }
 *
 *   public void onResourcesCompleted(List<SamzaResourceStatus> resourceStatus) {
 *     [check for exit code, and take actions]
 *   }
 *
 *   public void onResourcesWithdrawn(List<SamzaResource> resources) {}
 *
 *   public void onReboot() {}
 * }
 * }
 * </pre>
 *
 * The lifecycle of a ContainerProcessManager should be managed similarly to the following:
 *
 * <pre>
 * {@code
 * ContainerProcessManager processManager =
 *     new ContainerProcessManager(callback);
 * processManager.start();
 * [... request resources ...]
 * [... wait for application to complete ...]
 * processManager.stop();
 * }
 * </pre>
 */

/*
1.Investigate what it means to kill a StreamProcessor, and add it as an API here.
2.Consider an API for Container Process liveness - ie, to be notified when a StreamProcessor
joins or leaves the group
*/

public abstract class ContainerProcessManager
{
  protected final Callback _callback;

  public ContainerProcessManager(Callback callback)
  {
    _callback = callback;
  }

  public abstract void start();

  /***
   * Request resources for running container processes
   * @param resourceRequest
   */
  public abstract void requestResources (List<SamzaResourceRequest> resourceRequest);

  /***
   * Remove a previously submitted resource request. The previous container request may
   * have been submitted. Even after the remove request, a ContainerProcessManagerCallback
   * implementation must be prepared to receive an allocation for the previous request.
   * This is merely a best effort cancellation.
   *
   * @param request, the resource request that must be cancelled
   */
  public abstract void cancelResourceRequest (SamzaResourceRequest request);


  /***
   * If the app cannot use the resource or wants to give up the resource, it can release them.
   * @param resources
   */
  public abstract void releaseResources (List<SamzaResource> resources);

  /***
   * Requests the launch of a StreamProcessor with the specified context on the resource.
   * @param resource
   * @param builder A builder implementation that encapsulates the context for the
   *                StreamProcessor. A builder encapsulates the ID for the processor, the
   *                build environment, the command to execute etc.
   * @throws SamzaContainerLaunchException
   *
   */
  public abstract void launchStreamProcessor (SamzaResource resource, CommandBuilder builder) throws SamzaContainerLaunchException;


  public abstract void stop();

  /***
   *Defines a callback interface for interacting with notifications from a ContainerProcessManager
   */
  public interface Callback {

    /***
     * This callback is invoked when there are resources that are to be offered to the application.
     * Often, resources that an app requests may not be available. The application must be prepared
     * to handle callbacks for resources that it did not request.
     * @param resources that are offered to the application
     */
    public void onResourcesAvailable (List<SamzaResource> resources);

    /***
     * This callback is invoked when resources are no longer available to the application. A
     * resource could be marked 'completed' in scenarios like - failure of disk on the host,
     * pre-emption of the resource to another StreamProcessor, exit or termination of the
     * StreamProcessor running in the resource.
     *
     * The SamzaResourceStatus contains diagnostics on why the failure occured
     * @param resources statuses for the resources that were completed.
     */
    public void onResourcesCompleted (List<SamzaResourceStatus> resources);

    /***
     * This callback is invoked when there is an error in the ContainerProcessManager. This is
     * guaranteed to be invoked when there is an uncaught exception in any other
     * ContainerProcessManager callbacks.
     * @param e
     */
    public void onError (Throwable e);
  }
}




