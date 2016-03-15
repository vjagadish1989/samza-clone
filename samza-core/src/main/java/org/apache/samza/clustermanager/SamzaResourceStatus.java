package org.apache.samza.clustermanager;

/**
 * <p><code>SamzaResourceStatus</code> represents the current status of a
 * <code>StreamProcessor</code> and the resource it is on.</p>
 *
 * <p>It provides details such as:
 *   <ul>
 *     <li><code>resourceID</code> of the resource.</li>
 *     <li><em>Exit status</em> of the StreamProcessor.</li>
 *     <li><em>Diagnostic</em> message for a failed/pre-empted StreamProcessor.</li>
 *   </ul>
 * </p>
 *
 *
 * The exact semantics of various exit codes and failure modes are evolving.
 * Currently the following failures are handled -  termination of a process running in the resource,
 * resource preemption, disk failures on host.
 *
 */
public class SamzaResourceStatus
{
  String resourceID;
  String diagnostics;
  int exitCode;

  /**
   * Indicates that the StreamProcessor on the resource successfully completed.
   */
  public static final int SUCCESS=0;
  /**
   * Indicates the failure of the StreamProcessor running on the resource.
   */
  public static final int ABORTED=1;
  /**
   * Indicates that the resource was preempted (given to another processor) by
   * the cluster manager
   */
  public static final int PREEMPTED=2;
  /**
   * Indicates a disk failure in the host the resource is on.
   * Currently these are modelled after Yarn, could evolve as we add integrations with
   * many cluster managers.
   */
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
