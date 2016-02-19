package org.apache.samza.clustermanager;

/**
 * Created by jvenkatr on 2/4/16.
 */
public class SamzaResource {
    int numCores;
    int memoryMb;
    String host;
    String resourceID;

    public SamzaResource(int numCores, int memoryMb, String host, String resourceID) {
        this.numCores = numCores;
        this.memoryMb = memoryMb;
        this.host = host;
        this.resourceID = resourceID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SamzaResource resource = (SamzaResource) o;

        if (numCores != resource.numCores) return false;
        if (memoryMb != resource.memoryMb) return false;
        return resourceID.equals(resource.resourceID);

    }

    @Override
    public int hashCode() {
        int result = numCores;
        result = 31 * result + memoryMb;
        result = 31 * result + resourceID.hashCode();
        return result;
    }

    public int getNumCores() {

        return numCores;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public String getHost() {
        return host;
    }

    public String getResourceID() {
        return resourceID;
    }
}
