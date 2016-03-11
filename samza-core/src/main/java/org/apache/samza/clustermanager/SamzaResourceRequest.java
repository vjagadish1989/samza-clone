package org.apache.samza.clustermanager;

import org.omg.CORBA.OBJ_ADAPTER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specification of a Request for resources from a ContainerProcessManager. A
 * resource request currently includes cpu cores and memory in MB. A preferred host
 * can also be specified with a request.
 */
public class SamzaResourceRequest implements Comparable<SamzaResourceRequest>
{
  int numCores;
  int memoryMB;
  String preferredHost;
  String requestID;
  int expectedContainerID;
  long requestTimestamp;

  private static final Logger log = LoggerFactory.getLogger(SamzaResourceRequest.class);

  public int getExpectedContainerID() {
      return expectedContainerID;
  }

  public void setExpectedContainerID(int expectedContainerID) {
      this.expectedContainerID = expectedContainerID;
  }

  public long getRequestTimestamp() {
      return requestTimestamp;
  }

  public void setRequestTimestamp(long requestTimestamp) {
      this.requestTimestamp = requestTimestamp;
  }

  public SamzaResourceRequest(int numCores, int memoryMB, String preferredHost, String requestID, int expectedContainerID) {
      this.numCores = numCores;
      this.memoryMB = memoryMB;
      this.preferredHost = preferredHost;
      this.requestID = requestID;
      this.expectedContainerID = expectedContainerID;
      this.requestTimestamp = System.currentTimeMillis();
      log.info("Resource Request created for {} on {} at {}", new Object[] {this.expectedContainerID, this.preferredHost, this.requestTimestamp}  );
  }

  public String getRequestID() {
      return requestID;
  }

  public void setRequestID(String requestID) {
      this.requestID = requestID;
  }

  public int getNumCores() {
      return numCores;
  }

  public void setNumCores(int numCores) {
      this.numCores = numCores;
  }

  public String getPreferredHost() {
      return preferredHost;
  }

  public void setPreferredHost(String preferredHost) {
      this.preferredHost = preferredHost;
  }

  public int getMemoryMB() {
      return memoryMB;
  }

  public void setMemoryMB(int memoryMB) {
      this.memoryMB = memoryMB;
  }

  @Override
  public String toString() {
      return "SamzaResourceRequest{" +
              "numCores=" + numCores +
              ", memoryMB=" + memoryMB +
              ", preferredHost='" + preferredHost + '\'' +
              ", requestID='" + requestID + '\'' +
              ", expectedContainerID=" + expectedContainerID +
              ", requestTimestamp=" + requestTimestamp +
              '}';
  }

  @Override
  public int compareTo(SamzaResourceRequest o) {
      if(this.requestTimestamp < o.requestTimestamp)
          return -1;
      if(this.requestTimestamp > o.requestTimestamp)
          return 1;
      return 0;
  }
}
