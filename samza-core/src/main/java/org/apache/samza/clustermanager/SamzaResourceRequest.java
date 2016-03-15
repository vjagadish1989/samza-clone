package org.apache.samza.clustermanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specification of a Request for resources from a ContainerProcessManager. A
 * resource request currently includes cpu cores and memory in MB. A preferred host
 * can also be specified with a request.
 *
 * When used with a ordered data structures (for example, priority queues)
 * ordering between two SamzaResourceRequests is defined by their timestamp.
 *
 * //TODO: Define a SamzaResourceRequestBuilder API as specified in SAMZA-881
 */
public class SamzaResourceRequest implements Comparable<SamzaResourceRequest>
{
  /**
   * Specifications of a resource request.
   */
  private final int numCores;
  private final int memoryMB;
  /**
   * The preferred host on which the resource must be allocated. Can be set to
   * ContainerRequestState.ANY_HOST if there are no host preferences
   */
  private final String preferredHost;
  /**
   * A request is identified by an unique identifier.
   */
  private final String requestID;
  /**
   * The ID of the StreamProcessor which this request is for.
   */
  private final int expectedContainerID;

  /**
   * The timestamp in millis when the request was created.
   */
  private final long requestTimestampMs;

  private static final Logger log = LoggerFactory.getLogger(SamzaResourceRequest.class);

  public SamzaResourceRequest(int numCores, int memoryMB, String preferredHost, String requestID, int expectedContainerID) {
      this.numCores = numCores;
      this.memoryMB = memoryMB;
      this.preferredHost = preferredHost;
      this.requestID = requestID;
      this.expectedContainerID = expectedContainerID;
      this.requestTimestampMs = System.currentTimeMillis();
      log.info("Resource Request created for {} on {} at {}", new Object[] {this.expectedContainerID, this.preferredHost, this.requestTimestampMs}  );
  }

  public int getExpectedContainerID() {
    return expectedContainerID;
  }

  public long getRequestTimestampMs() {
    return requestTimestampMs;
  }

  public String getRequestID() {
      return requestID;
  }

  public int getNumCores() {
      return numCores;
  }

  public String getPreferredHost() {
      return preferredHost;
  }

  public int getMemoryMB() {
      return memoryMB;
  }

  @Override
  public String toString() {
      return "SamzaResourceRequest{" +
              "numCores=" + numCores +
              ", memoryMB=" + memoryMB +
              ", preferredHost='" + preferredHost + '\'' +
              ", requestID='" + requestID + '\'' +
              ", expectedContainerID=" + expectedContainerID +
              ", requestTimestampMs=" + requestTimestampMs +
              '}';
  }

  /**
   * Requests are ordered by the time at which they were created.
   * @param o
   * @return
   */
  @Override
  public int compareTo(SamzaResourceRequest o) {
      if(this.requestTimestampMs < o.requestTimestampMs)
          return -1;
      if(this.requestTimestampMs > o.requestTimestampMs)
          return 1;
      return 0;
  }
}
