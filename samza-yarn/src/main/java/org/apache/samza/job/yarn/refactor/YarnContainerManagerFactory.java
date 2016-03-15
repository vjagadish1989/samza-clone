package org.apache.samza.job.yarn.refactor;

import org.apache.samza.clustermanager.ContainerManagerFactory;
import org.apache.samza.clustermanager.ContainerProcessManager;
import org.apache.samza.clustermanager.SamzaAppState;
import org.apache.samza.coordinator.JobModelReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A YarnContainerProcessManagerFactory returns an implementation of a {@link ContainerProcessManager} for Yarn.
 */
public class YarnContainerManagerFactory implements ContainerManagerFactory {

    private static Logger log = LoggerFactory.getLogger(YarnContainerManagerFactory.class);
    @Override
    public ContainerProcessManager getContainerProcessManager(JobModelReader reader, ContainerProcessManager.Callback callback, SamzaAppState state) {
        log.info("Creating a YarnContainerProcess manager. ");

        YarnContainerManager manager = new YarnContainerManager(reader.jobModel().getConfig(),reader, callback, state);
        return manager;
    }
}
