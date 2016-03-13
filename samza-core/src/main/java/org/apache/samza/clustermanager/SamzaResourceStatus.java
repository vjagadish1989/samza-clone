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
 * The exact semantics of various exit codes and failure modes are evolving.
 *
 */
public class SamzaResourceStatus
{
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
