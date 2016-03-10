package org.apache.samza.config;

/**
 * Created by jvenkatr on 3/8/16.
 */
public class ClusterManagerConfig extends MapConfig{

    private static final String CLUSTER_JOBCOORDINATOR_SLEEP_INTERVAL = "samza.cluster-manager.sleep.interval.ms";
    private static final String IS_JMX_ENABLED = "samza.cluster-manager.jmx.enabled";

    private static final int DEFAULT_SLEEP_INTERVAL = 1000;

    private static final String CLUSTER_MANAGER_FACTORY = "samza.cluster-manager.factory";
    private static final String CLUSTER_MANAGER_FACTORY_DEFAULT = "org.apache.samza.job.yarn.refactor.YarnContainerManagerFactory";


    public ClusterManagerConfig (Config config) {
        super(config);
    }

    public int getJobCoordinatorSleepInterval() {
        return getInt(CLUSTER_JOBCOORDINATOR_SLEEP_INTERVAL, DEFAULT_SLEEP_INTERVAL);
    }

    public String getContainerManagerClass()
    {
        return get(CLUSTER_MANAGER_FACTORY, CLUSTER_MANAGER_FACTORY_DEFAULT);
    }

    public boolean getJmxServerEnabled() {
        return getBoolean(IS_JMX_ENABLED, true);
    }
}
