/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.job.yarn.refactor

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.samza.SamzaException
import org.apache.samza.clustermanager.SamzaAppState.SamzaAppStatus
import org.apache.samza.job.yarn.YarnAppMasterListener
import org.apache.samza.util.Logging

/**
 * Responsible for managing the lifecycle of the Yarn application master. Mostly,
 * this means registering and unregistering with the RM, and shutting down
 * when the RM tells us to Reboot.
 */
class SamzaYarnAppMasterLifecycle(containerMem: Int, containerCpu: Int, state: YarnAppState, amClient: AMRMClientAsync[ContainerRequest]) extends Logging {
  var validResourceRequest = true
  var shutdownMessage: String = null
  var webApp: SamzaAppMasterService = null
  def onInit() {
    val host = state.nodeHost
    val response = amClient.registerApplicationMaster(host, state.rpcUrl.getPort, "%s:%d" format (host, state.trackingUrl.getPort))

    // validate that the YARN cluster can handle our container resource requirements
    val maxCapability = response.getMaximumResourceCapability
    val maxMem = maxCapability.getMemory
    val maxCpu = maxCapability.getVirtualCores
    info("Got AM register response. The YARN RM supports container requests with max-mem: %s, max-cpu: %s" format (maxMem, maxCpu))

    if (containerMem > maxMem || containerCpu > maxCpu) {
      shutdownMessage = "The YARN cluster is unable to run your job due to unsatisfiable resource requirements. You asked for mem: %s, and cpu: %s." format (containerMem, containerCpu)
      error(shutdownMessage)
      validResourceRequest = false
      state.yarnContainerManagerStatus = FinalApplicationStatus.FAILED
      state.samzaAppState.jobHealthy.set(false)
    }
  }

  def onReboot() {
    throw new SamzaException("Received a reboot signal from the RM, so throwing an exception to reboot the AM.")
  }

  def onShutdown(samzaAppStatus: SamzaAppStatus) {
    info("Shutting down SamzaAppStatus: " + samzaAppStatus + " YarnContainerManagerStatus: " + state.yarnContainerManagerStatus )
    //The value of state.status is set to either SUCCEEDED or FAILED for errors we catch and handle - like container failures
    //All other AM failures (errors in callbacks/connection failures after retries/token expirations) should not unregister the AM,
    //allowing the RM to restart it (potentially on a different host)
    if(samzaAppStatus != SamzaAppStatus.UNDEFINED && state.yarnContainerManagerStatus != FinalApplicationStatus.UNDEFINED) {
      info("Unregistering AM from the RM.")
      amClient.unregisterApplicationMaster(state.yarnContainerManagerStatus, shutdownMessage, null)
      info("Unregister complete.")
    }
    else {
      info("Not unregistering AM from the RM. This will enable RM retries")
    }
  }

  def shouldShutdown = !validResourceRequest
}
