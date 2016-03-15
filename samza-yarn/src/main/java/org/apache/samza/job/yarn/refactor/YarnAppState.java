package org.apache.samza.job.yarn.refactor;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.samza.clustermanager.SamzaAppState;
import org.apache.samza.coordinator.JobModelReader;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * YarnAppState encapsulates Yarn specific state variables that are Yarn specific. This class
 * is useful for information to display in the UI.
 *
 * TODO: make these variables private, provide thread-safe accessors.
 */
public class YarnAppState {

   public final SamzaAppState samzaAppState;
/*  The following state variables are primarily used for reference in the AM web services   */

  /**
   * Task Id of the AM
   * Used for displaying in the AM UI. Usage found in {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   * and scalate/WEB-INF/views/index.scaml
   */
  public final JobModelReader jobModelReader;

  public final int taskId;
  /**
   * Id of the AM container (as allocated by the RM)
   * Used for displaying in the AM UI. Usage in {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   * and scalate/WEB-INF/views/index.scaml
   */
  public final ContainerId amContainerId;
  /**
   * Host name of the NM on which the AM is running
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public final String nodeHost;
  /**
   * NM port on which the AM is running
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public final int nodePort;
  /**
   * Http port of the NM on which the AM is running
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public final int nodeHttpPort;
  /**
   * Application Attempt Id as provided by Yarn
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   * and {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   */
  public final ApplicationAttemptId appAttemptId;
  /**
   * Job Coordinator URL
   * Usage in {@link org.apache.samza.job.yarn.SamzaAppMasterService} &amp; YarnContainerRunner
   */
  public URL coordinatorUrl = null;
  /**
   * URL of the {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   */
  public URL rpcUrl = null;
  /**
   * URL of the {@link org.apache.samza.webapp.ApplicationMasterWebServlet}
   */
  public URL trackingUrl = null;

  public Set<Container> runningContainers = new HashSet<Container>()  ;

  /**
  * Final status of the application
  * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
  */
  public FinalApplicationStatus yarnContainerManagerStatus = FinalApplicationStatus.UNDEFINED;

  /**
  * State indicating whether the job is healthy or not
  * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
  */


  public YarnAppState(JobModelReader jobModelReader,
                    int taskId,
                    ContainerId amContainerId,
                    String nodeHost,
                    int nodePort,
                    int nodeHttpPort,
                    SamzaAppState state) {
    this.jobModelReader = jobModelReader;
    this.taskId = taskId;
    this.amContainerId = amContainerId;
    this.nodeHost = nodeHost;
    this.nodePort = nodePort;
    this.nodeHttpPort = nodeHttpPort;
    this.appAttemptId = amContainerId.getApplicationAttemptId();
    this.samzaAppState = state;
  }

  @Override
  public String toString() {
    return "YarnAppState{" +
            "jobCoordinator=" + jobModelReader +
            ", taskId=" + taskId +
            ", amContainerId=" + amContainerId +
            ", nodeHost='" + nodeHost + '\'' +
            ", nodePort=" + nodePort +
            ", nodeHttpPort=" + nodeHttpPort +
            ", appAttemptId=" + appAttemptId +
            ", coordinatorUrl=" + coordinatorUrl +
            ", rpcUrl=" + rpcUrl +
            ", trackingUrl=" + trackingUrl +
            ", runningContainers=" + runningContainers +
            ", status=" + yarnContainerManagerStatus +
            '}';
  }
}