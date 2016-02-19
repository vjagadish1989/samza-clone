package org.apache.samza.clustermanager;

/**
 * Created by jvenkatr on 2/4/16.
 */
public interface SamzaJobCoordinatorFactory {
    public SamzaJobCoordinator getJobCoordinator();
}
