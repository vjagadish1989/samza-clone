package org.apache.samza.coordinator;

/**
 * Created by jvenkatr on 2/4/16.
 */
public class SamzaResourceStatus {
    String resourceID;
    String diagnostics;
    int exitCode;

    public static final int SUCCESS=0;
    public static final int ABORTED=1;
    public static final int PREEMPTED=2;
    public static final int DISK_FAIL=3;

    public SamzaResourceStatus(String resourceID, String diagnostics, int exitCode) {
        this.resourceID = resourceID;
        this.diagnostics = diagnostics;
        this.exitCode = exitCode;
    }

    public int getExitCode() {
        return exitCode;
    }

    public void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getResourceID() {
        return resourceID;
    }

    public void setResourceID(String resourceID) {
        this.resourceID = resourceID;
    }

    @Override
    public String toString() {
        return "SamzaResourceStatus{" +
                "resourceID='" + resourceID + '\'' +
                ", diagnostics='" + diagnostics + '\'' +
                ", exitCode=" + exitCode +
                '}';
    }
}
