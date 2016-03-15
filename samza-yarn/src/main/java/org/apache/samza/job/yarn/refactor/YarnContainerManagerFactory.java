package org.apache.samza.job.yarn.refactor;

import org.apache.samza.clustermanager.ContainerManagerFactory;
import org.apache.samza.clustermanager.ContainerProcessManager;
import org.apache.samza.coordinator.JobModelReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jvenkatr on 3/8/16.
 */
public class YarnContainerManagerFactory implements ContainerManagerFactory {

    private static Logger log = LoggerFactory.getLogger(YarnContainerManagerFactory.class);
    @Override
    public ContainerProcessManager getContainerProcessManager(JobModelReader reader, ContainerProcessManager.Callback callback) {
        log.info("inside factory1 ");

        YarnContainerManager manager = new YarnContainerManager(reader.jobModel().getConfig(),reader, callback);
        return manager;
    }
}
