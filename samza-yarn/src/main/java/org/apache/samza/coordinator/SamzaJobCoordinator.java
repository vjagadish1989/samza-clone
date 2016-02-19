package org.apache.samza.coordinator;

import org.apache.samza.job.model.JobModel;

import java.io.IOException;

/**
 * Created by jvenkatr on 2/4/16.
 */
public interface SamzaJobCoordinator {
    public void start() throws IOException;
    public int getContainerId();
    public JobModel getJobModel();
    public void stop();
}
