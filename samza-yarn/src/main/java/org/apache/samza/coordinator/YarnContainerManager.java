package org.apache.samza.coordinator;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.coordinator.*;
import org.apache.samza.job.coordinator.SamzaAppMasterService;
import org.apache.samza.job.yarn.*;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jvenkatr on 2/4/16.
 */
public class YarnContainerManager implements ContainerProcessManager, AMRMClientAsync.CallbackHandler {

    AMRMClientAsync<AMRMClient.ContainerRequest> amClient;
    ContainerProcessManagerCallback _callback;
    ContainerUtil util;
    YarnConfiguration hConfig;
    YarnAppState state;

    SamzaYarnAppMasterLifecycle lifecycle;
    SamzaAppMasterService service;

    private static final Logger log = LoggerFactory.getLogger(YarnContainerManager.class);
    Map<SamzaResource, Container> allocatedResources = new HashMap<SamzaResource, Container>();
    Map<SamzaResourceRequest, AMRMClient.ContainerRequest> requestsMap = new HashMap<>();

    //      val state = new SamzaAppState(jobCoordinator, -1, containerId, nodeHostString, nodePortString.toInt, nodeHttpPortString.toInt)

    public YarnContainerManager (Config config, JobCoordinator coordinator, ContainerProcessManagerCallback callback ) {
        _callback = callback;

        hConfig = new YarnConfiguration();
        hConfig.set("fs.http.impl", HttpFileSystem.class.getName());
        ClientHelper clientHelper = new ClientHelper(hConfig);
        MetricsRegistryMap registry = new MetricsRegistryMap();

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

        this.state = new YarnAppState(coordinator, -1, containerId, nodeHostString, nodePort, nodeHttpPort);
        log.info(state.toString());

        this.service = new SamzaAppMasterService(config, this.state, registry);
        service.onInit();



        log.info("containerID str {}, nodehoststring {} , nodeport string {} , nodehtpport {}"+ containerIdStr + " " + nodeHostString + " " + nodePort + " " + nodeHttpPort);
        this.lifecycle = new SamzaYarnAppMasterLifecycle(yarnConfig.getContainerMaxMemoryMb(), yarnConfig.getContainerMaxCpuCores(), state, amClient );

        util = new ContainerUtil(config, hConfig);
    }

    @Override
    public void start() {
        //      val state = new SamzaAppState(jobCoordinator, -1, containerId, nodeHostString, nodePortString.toInt, nodeHttpPortString.toInt)

        amClient.init(hConfig);
        amClient.start();
        lifecycle.onInit();

    }


    @Override
    public void requestResources(List<SamzaResourceRequest> resourceRequests, ContainerProcessManagerCallback callback) {
        int DEFAULT_PRIORITY = 0;
        for(SamzaResourceRequest resourceRequest : resourceRequests) {
            log.info("request resources called " + resourceRequest);

            int memoryMb = resourceRequest.getMemoryMB();
            int cpuCores = resourceRequest.getNumCores();
            String preferredHost = resourceRequest.getPreferredHost();
            Resource capability = Resource.newInstance(memoryMb, cpuCores);
            Priority priority =  Priority.newInstance(DEFAULT_PRIORITY);
            String hosts[] = null;
            if (preferredHost != null) {
                hosts = new String[] {preferredHost};
            }
            AMRMClient.ContainerRequest issuedRequest = new AMRMClient.ContainerRequest(capability, hosts, null, priority);
            requestsMap.put(resourceRequest, issuedRequest);
            amClient.addContainerRequest(issuedRequest);
        }
    }

    @Override
    public void releaseResources(List<SamzaResource> resources, ContainerProcessManagerCallback callback) {
             for(SamzaResource resource : resources) {
                 log.info("release resource called " + resource);
                 Container container = allocatedResources.get(resource);
                 state.runningContainers.remove(container);
                 amClient.releaseAssignedContainer(container.getId());
             }
    }

    @Override
    public void launchStreamProcessor(SamzaResource resource, int containerID, CommandBuilder builder) {
        log.info("received launch req for " + resource + " on ID : " + containerID + " with builder " + builder
        );
        Container container = allocatedResources.get(resource);
        state.runningContainers.add(container);
        util.runContainer(containerID, container, builder);
    }

    @Override
    public void cancelResourceRequest(SamzaResourceRequest request) {
        AMRMClient.ContainerRequest containerRequest = requestsMap.get(request);
        amClient.removeContainerRequest(containerRequest);
    }

    @Override
    public void stop() {
        log.info("stopping am client " );
        lifecycle.onShutdown();
        amClient.stop();
        service.onShutdown();
    }




    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        List<SamzaResourceStatus> samzaResrcStatuses = new ArrayList<>();

        for(ContainerStatus status: statuses) {
            log.info("Container completed from RM " + status);

            SamzaResourceStatus samzaResrcStatus = new SamzaResourceStatus(status.getContainerId().toString(), status.getDiagnostics(), status.getExitStatus());
            samzaResrcStatuses.add(samzaResrcStatus);
        }
        _callback.onResourcesCompleted(samzaResrcStatuses);
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        List<SamzaResource> resources = new ArrayList<SamzaResource>();
        for(Container container : containers) {
            log.info("Container allocated from RM " + container);
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

    @Override
    public void onError(Throwable e) {
           throw new SamzaException(e);
    }
}
