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

package org.apache.samza.clustermanager;

import org.apache.samza.coordinator.JobModelManager;

/**
 * A factory to build a {@link ClusterResourceManager}
 * //TODO: move the class to Samza-API?
 */
public interface ResourceManagerFactory
{
  /**
   * Return a {@link ClusterResourceManager }
   * @param callback to be registered with the {@link ClusterResourceManager}
   * @param state Useful if the ClusterResourceManager wants to host an UI.
   * //TODO: Remove the SamzaAppState param and refactor into a smaller focussed class.
   * //TODO: Investigate the possibility a common Samza UI for all cluster managers - Yarn,Mesos,Standalone
   * @return the instantiated {@link ClusterResourceManager}
   */
  public ClusterResourceManager getClusterResourceManager(ClusterResourceManager.Callback callback, SamzaAppState state);
}
