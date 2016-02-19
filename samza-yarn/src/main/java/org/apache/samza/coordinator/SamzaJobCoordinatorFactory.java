package org.apache.samza.coordinator;

/**
 * Created by jvenkatr on 2/4/16.
 */
public interface SamzaJobCoordinatorFactory {
    public SamzaJobCoordinator getJobCoordinator();
}
