package org.apache.samza.coordinator;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jvenkatr on 2/4/16.
 */
public class YarnAppState {

  /*  The following state variables are primarily used for reference in the AM web services   */
        /**
         * Task Id of the AM
         * Used for displaying in the AM UI. Usage found in {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
         * and scalate/WEB-INF/views/index.scaml
         */
        public final JobCoordinator jobCoordinator;

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
         * JMX Server URL, if enabled
         * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
         */
        public String jmxUrl = "";
        /**
         * JMX Server Tunneling URL, if enabled
         * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
         */
        public String jmxTunnelingUrl = "";
        /**
         * Job Coordinator URL
         * Usage in {@link org.apache.samza.job.yarn.SamzaAppMasterService} &amp; ContainerUtil
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
    public FinalApplicationStatus status = FinalApplicationStatus.UNDEFINED;

    /**
     * State indicating whether the job is healthy or not
     * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
     */
    public AtomicBoolean jobHealthy = new AtomicBoolean(true);


    public YarnAppState(JobCoordinator jobCoordinator,
                         int taskId,
                         ContainerId amContainerId,
                         String nodeHost,
                         int nodePort,
                         int nodeHttpPort) {
        this.jobCoordinator = jobCoordinator;
        this.taskId = taskId;
        this.amContainerId = amContainerId;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.nodeHttpPort = nodeHttpPort;
        this.appAttemptId = amContainerId.getApplicationAttemptId();

    }

    @Override
    public String toString() {
        return "YarnAppState{" +
                "jobCoordinator=" + jobCoordinator +
                ", taskId=" + taskId +
                ", amContainerId=" + amContainerId +
                ", nodeHost='" + nodeHost + '\'' +
                ", nodePort=" + nodePort +
                ", nodeHttpPort=" + nodeHttpPort +
                ", appAttemptId=" + appAttemptId +
                ", jmxUrl='" + jmxUrl + '\'' +
                ", jmxTunnelingUrl='" + jmxTunnelingUrl + '\'' +
                ", coordinatorUrl=" + coordinatorUrl +
                ", rpcUrl=" + rpcUrl +
                ", trackingUrl=" + trackingUrl +
                ", runningContainers=" + runningContainers +
                ", status=" + status +
                ", jobHealthy=" + jobHealthy +
                '}';
    }
}