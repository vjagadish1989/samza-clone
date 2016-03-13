package org.apache.samza.job.yarn.refactor;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.clustermanager.*;
import org.apache.samza.config.Config;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.coordinator.JobModelReader;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * An {@link YarnContainerManager} implements a ContainerProcessManager using Yarn as the underlying
 * resource manager. This class is as an adaptor between Yarn and translates Yarn callbacks into
 * Samza specific callback methods as specified in Callback.
 *
 */

public class YarnContainerManager extends ContainerProcessManager implements AMRMClientAsync.CallbackHandler {
  /**
   * The containerProcessManager instance to request resources from yarn.
   */
  private final AMRMClientAsync<AMRMClient.ContainerRequest> amClient;


  /**
   * A helper class to launch Yarn containers.
   */
  private final YarnContainerRunner yarnContainerRunner;

  private final YarnConfiguration hConfig;
  private final YarnAppState state;

  private final SamzaYarnAppMasterLifecycle lifecycle;
  private final SamzaAppMasterService service;

  private static final Logger log = LoggerFactory.getLogger(YarnContainerManager.class);

  /**
   * State variables to map Yarn specific callbacks into Samza specific callbacks.
   */
  final Map<SamzaResource, Container> allocatedResources = new HashMap<SamzaResource, Container>();
  final Map<SamzaResourceRequest, AMRMClient.ContainerRequest> requestsMap = new HashMap<>();


  public YarnContainerManager ( JobModelReader coordinator, Callback callback ) {
    this(coordinator.jobModel().getConfig(), coordinator, callback);
  }

  /**
   * Creates an YarnContainerManager from config, a jobModelReader and a callback.
   *
   * @param config
   * @param jobModelReader
   * @param callback callback to be invoked based on events from the ContainerProcessManager
   */
  public YarnContainerManager (Config config, JobModelReader jobModelReader, ContainerProcessManager.Callback callback )
  {
    super(callback);
    hConfig = new YarnConfiguration();
    hConfig.set("fs.http.impl", HttpFileSystem.class.getName());

    ClientHelper clientHelper = new ClientHelper(hConfig);
    MetricsRegistryMap registry = new MetricsRegistryMap();

    // parse configs from the Yarn environment
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
    String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.toString());
    String nodeHttpPortString = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.toString());
    int nodePort = Integer.parseInt(nodePortString);
    int nodeHttpPort = Integer.parseInt(nodeHttpPortString);
    YarnConfig yarnConfig = new YarnConfig(config);
    int interval = yarnConfig.getAMPollIntervalMs();
    this.amClient = AMRMClientAsync.createAMRMClientAsync(interval, this);

    this.state = new YarnAppState(jobModelReader, -1, containerId, nodeHostString, nodePort, nodeHttpPort);
    log.info("Initialized YarnAppState: {}", state.toString());

    this.service = new SamzaAppMasterService(config, this.state, registry);

    log.info("containerID str {}, nodehost  {} , nodeport  {} , nodehtpport {}", new Object [] {containerIdStr, nodeHostString, nodePort, nodeHttpPort});
    this.lifecycle = new SamzaYarnAppMasterLifecycle(yarnConfig.getContainerMaxMemoryMb(), yarnConfig.getContainerMaxCpuCores(), state, amClient );

    yarnContainerRunner = new YarnContainerRunner(config, hConfig);
  }

  /**
   * Starts the YarnContainerManager and initialize all its sub-systems.
   */
  @Override
  public void start()
  {
    service.onInit();
    log.info("Starting YarnContainerManager.");
    amClient.init(hConfig);
    amClient.start();
    lifecycle.onInit();
    log.info("Finished starting YarnContainerManager");
  }

  /**
   * Request resources for running container processes.
   * @param resourceRequests
   */
  @Override
  public void requestResources(List<SamzaResourceRequest> resourceRequests)
  {
    int DEFAULT_PRIORITY = 0;
    for(SamzaResourceRequest resourceRequest : resourceRequests)
    {
      log.info("Requesting resources on  " + resourceRequest.getPreferredHost() + " for container " + resourceRequest.getExpectedContainerID());

      int memoryMb = resourceRequest.getMemoryMB();
      int cpuCores = resourceRequest.getNumCores();
      String preferredHost = resourceRequest.getPreferredHost();
      Resource capability = Resource.newInstance(memoryMb, cpuCores);
      Priority priority =  Priority.newInstance(DEFAULT_PRIORITY);

      AMRMClient.ContainerRequest issuedRequest=null;

      if (preferredHost.equals("ANY_HOST"))
      {
        log.info("Making a request for ANY_HOST " + preferredHost );
        issuedRequest = new AMRMClient.ContainerRequest(capability, null, null, priority);
      }
      else
      {
        log.info("Making a preferred host request on " + preferredHost);
        issuedRequest = new AMRMClient.ContainerRequest(
                capability,
                new String[]{preferredHost},
                null,
                priority);
      }

      requestsMap.put(resourceRequest, issuedRequest);
      amClient.addContainerRequest(issuedRequest);
    }
  }

  /**
   * Requests the YarnContainerManager to release a resource. If the app cannot use the resource or wants to give up
   * the resource, it can release them.
   *
   * @param resources
   */

  @Override
  public void releaseResources(List<SamzaResource> resources)
  {
    for(SamzaResource resource : resources)
    {
      log.info("Release resource called {} ", resource);
      Container container = allocatedResources.get(resource);
      state.runningContainers.remove(container);
      amClient.releaseAssignedContainer(container.getId());
   }
  }

  /**
   *
   * Requests the launch of a StreamProcessor with the specified ID on the resource
   * @param resource , the SamzaResource on which to launch the StreamProcessor
   * @param builder, the builder to build the resource launch command from
   *
   * TODO: Support non-builder methods to launch resources. Maybe, refactor into a ContainerLaunchStrategy interface
   */

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) throws SamzaContainerLaunchException {
      String containerIDStr = builder.buildEnvironment().get(ShellCommandConfig.ENV_CONTAINER_ID());
      int containerID = Integer.parseInt(containerIDStr);
      log.info("received launch request for {} on hostname {}", containerID , resource.getHost());
      Container container = allocatedResources.get(resource);
      state.runningContainers.add(container);
      yarnContainerRunner.runContainer(containerID, container, builder);
  }

  /**
   *
   * Remove a previously submitted resource request. The previous container request may have
   * been submitted. Even after the remove request, a Callback implementation must
   * be prepared to receive an allocation for the previous request. This is merely a best effort cancellation.
   *
   * @param request the request to be cancelled
   */
  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {
    log.info("cacelling request {} ", request);
    AMRMClient.ContainerRequest containerRequest = requestsMap.get(request);
    amClient.removeContainerRequest(containerRequest);
  }


  /**
   * Stops the YarnContainerManager and all its sub-components
   */
  @Override
  public void stop() {
    log.info("Stopping AM client " );
    lifecycle.onShutdown();
    amClient.stop();
    log.info("Stopping the AM service " );
    service.onShutdown();
  }

  /**
   * Callback invoked from Yarn when containers complete. This translates the yarn callbacks into Samza specific
   * ones.
   *
   * @param statuses
   */
  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    List<StreamProcessorStatus> samzaResrcStatuses = new ArrayList<>();

    for(ContainerStatus status: statuses) {
      log.info("Container completed from RM " + status);

      StreamProcessorStatus samzaResrcStatus = new StreamProcessorStatus(status.getContainerId().toString(), status.getDiagnostics(), status.getExitStatus());
      samzaResrcStatuses.add(samzaResrcStatus);
    }
    _callback.onResourcesCompleted(samzaResrcStatuses);
  }

  /**
   * Callback invoked from Yarn when containers are allocated. This translates the yarn callbacks into Samza
   * specific ones.
   * @param containers
   */
  @Override
  public void onContainersAllocated(List<Container> containers) {
      List<SamzaResource> resources = new ArrayList<SamzaResource>();
      for(Container container : containers) {
          log.info("Container allocated from RM on " + container.getNodeId().getHost());
          final String id = container.getId().toString();
          String host = container.getNodeId().getHost();
          int memory = container.getResource().getMemory();
          int numCores = container.getResource().getVirtualCores();

          SamzaResource resource = new SamzaResource(numCores, memory, host, id);
          allocatedResources.put(resource, container);
          resources.add(resource);
      }
      _callback.onResourcesAvailable(resources);
  }

  @Override
  public void onShutdownRequest() {

  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {

  }

  @Override
  public float getProgress() {
      return 0;
  }

  /**
   * Callback invoked when there is an error in the Yarn client. This delegates the
   * callback handling to the ContainerProcessManager callback instance.
   *
   * @param e
   */
  @Override
  public void onError(Throwable e) {
    log.error("Exception in the Yarn callback {}", e);
    _callback.onError(e);
  }
}
