package org.apache.samza.coordinator;

import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.samza.job.CommandBuilder;

import java.util.*;

/**
 * Created by jvenkatr on 2/5/16.
 */
public class MesosContainerManager implements  ContainerProcessManager, Scheduler {

    MesosSchedulerDriver driver;
    Map<SamzaResource, Protos.Offer> offerMap = new HashMap<SamzaResource, Protos.Offer>();
    Map<Protos.Offer, SamzaResource> resourcesMap = new HashMap<Protos.Offer, SamzaResource>();
    Map<Protos.OfferID,Protos.Offer > offersById = new HashMap<Protos.OfferID, Protos.Offer>();

    ContainerProcessManagerCallback _callback;

    @Override
    public void start() {
      driver.run();
    }

    @Override
    public void requestResources(List<SamzaResourceRequest> resourceRequests, ContainerProcessManagerCallback callback) {
        for(SamzaResourceRequest samzaRequest : resourceRequests)           {
        List<Protos.Request> requests = new ArrayList<Protos.Request>();
        Protos.Resource cpuResource =     buildCPUResource(samzaRequest.getNumCores());
        Protos.Resource memResource =  buildMemResource(samzaRequest.getMemoryMB());
        driver.requestResources(requests);
    }
    }

    @Override
    public void releaseResources(List<SamzaResource> resources, ContainerProcessManagerCallback callback) {
        // NO-OP
    }

    @Override
    public void launchStreamProcessor(SamzaResource resource, int containerID, CommandBuilder builder) {

    }

    @Override
    public void cancelResourceRequest(SamzaResourceRequest request) {

    }

    @Override
    public void stop() {

    }

    /////////

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {

    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {

    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        List<SamzaResource> resources = new ArrayList<SamzaResource>();
        for (Protos.Offer offer : offers) {
            String hostName = offer.getHostname();
            String offerID = offer.getId().toString();
            List<Protos.Resource> resourcesList = offer.getResourcesList();
            double cpus=0, mem=0;
            for(Protos.Resource resource : resourcesList) {
               if(resource.getName().equals("cpus")) {
                   cpus = resource.getScalar().getValue();
               }
                if(resource.getName().equals("mem")) {
                  mem =  resource.getScalar().getValue();
                }
                SamzaResource samzaResource = new SamzaResource(0,0, hostName, offerID);
                resources.add(samzaResource);
                offerMap.put(samzaResource, offer);
                resources.add(samzaResource);
            }

        }
        _callback.onResourcesAvailable(resources);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        Protos.Offer offer = offersById.get(offerId);
        SamzaResource samzaResource = resourcesMap.get(offer);
        List<SamzaResource> resources = new ArrayList<SamzaResource>();
        resources.add(samzaResource);
        _callback.onResourcesWithdrawn(resources);
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {

    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {

    }

    @Override
    public void disconnected(SchedulerDriver driver) {

    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {

    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {

    }

    @Override
    public void error(SchedulerDriver driver, String message) {

    }

    public static Protos.Resource buildCPUResource(double cpus) {
        return Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .build();
    }

    public static Protos.Resource buildMemResource(double mem) {
        return Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .build();
    }


}
