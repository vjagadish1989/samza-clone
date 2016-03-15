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
package org.apache.samza.job.yarn.refactor;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.SamzaContainerLaunchException;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.ShellCommandBuilder;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * A Helper class to run container processes on Yarn. This encapsulates quite a bit of YarnContainer
 * boiler plate.
 */
public class YarnContainerRunner {
  private static final Logger log = LoggerFactory.getLogger(YarnContainerRunner.class);

  private final Config config;
  private final YarnConfiguration yarnConfiguration;

  private NMClient nmClient;
  private final YarnConfig yarnConfig;
  private final TaskConfig taskConfig;

  /**
   * Create a new Runner from a Config.
   * @param config
   * @param yarnConfiguration
   */
  public YarnContainerRunner(Config config,
                             YarnConfiguration yarnConfiguration) {
    this.config = config;
    this.yarnConfiguration = yarnConfiguration;

    this.nmClient = NMClient.createNMClient();
    nmClient.init(this.yarnConfiguration);

    this.yarnConfig = new YarnConfig(config);
    this.taskConfig = new TaskConfig(config);
  }

  protected void setNmClient(NMClient nmClient){
    this.nmClient = nmClient;
  }




  public void runContainer(int samzaContainerId, Container container, CommandBuilder cmdBuilder) throws SamzaContainerLaunchException {
    String containerIdStr = ConverterUtils.toString(container.getId());
    log.info("Got available container ID ({}) for container: {}", samzaContainerId, container);

    String cmdBuilderClassName;
    if (taskConfig.getCommandClass().isDefined()) {
      cmdBuilderClassName = taskConfig.getCommandClass().get();
    } else {
      cmdBuilderClassName = ShellCommandBuilder.class.getName();
    }

    String command = cmdBuilder.buildCommand();
    log.info("Container ID {} using command {}", samzaContainerId, command);

    Map<String, String> env = getEscapedEnvironmentVariablesMap(cmdBuilder);
    printContainerEnvironmentVariables(samzaContainerId, env);

    Path path = new Path(yarnConfig.getPackagePath());
    log.info("Starting container ID {} using package path {}", samzaContainerId, path);

    startContainer(
        path,
        container,
        env,
        getFormattedCommand(
            ApplicationConstants.LOG_DIR_EXPANSION_VAR,
            command,
            ApplicationConstants.STDOUT,
            ApplicationConstants.STDERR)
    );


    log.info("Claimed container ID {} for container {} on node {} (http://{}/node/containerlogs/{}).",
        new Object[]{
            samzaContainerId,
            containerIdStr,
            container.getNodeId().getHost(),
            container.getNodeHttpAddress(),
            containerIdStr}
    );

      log.info("Started container ID {}", samzaContainerId);
  }

  private void startContainer(Path packagePath,
                                Container container,
                                Map<String, String> env,
                                final String cmd) throws SamzaContainerLaunchException {
    log.info("starting container {} {} {} {}",
        new Object[]{packagePath, container, env, cmd});

    // set the local package so that the containers and app master are provisioned with it
    LocalResource packageResource = Records.newRecord(LocalResource.class);
    URL packageUrl = ConverterUtils.getYarnUrlFromPath(packagePath);
    FileStatus fileStatus;
    try {
      fileStatus = packagePath.getFileSystem(yarnConfiguration).getFileStatus(packagePath);
    } catch (IOException ioe) {
      log.error("IO Exception when accessing the package status from the filesystem", ioe);
      throw new SamzaContainerLaunchException("IO Exception when accessing the package status from the filesystem");
    }

    packageResource.setResource(packageUrl);
    packageResource.setSize(fileStatus.getLen());
    packageResource.setTimestamp(fileStatus.getModificationTime());
    packageResource.setType(LocalResourceType.ARCHIVE);
    packageResource.setVisibility(LocalResourceVisibility.APPLICATION);

    ByteBuffer allTokens;
    // copy tokens (copied from dist shell example)
    try {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);

      // now remove the AM->RM token so that containers cannot access it
      Iterator iter = credentials.getAllTokens().iterator();
      while (iter.hasNext()) {
        TokenIdentifier token = ((Token) iter.next()).decodeIdentifier();
        if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          iter.remove();
        }
      }
      allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new SamzaContainerLaunchException("IO Exception when writing credentials to output buffer");
    }

    ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
    context.setEnvironment(env);
    context.setTokens(allTokens.duplicate());
    context.setCommands(new ArrayList<String>() {{add(cmd);}});
    context.setLocalResources(Collections.singletonMap("__package", packageResource));

    log.debug("setting package to {}", packageResource);
    log.debug("setting context to {}", context);

    StartContainerRequest startContainerRequest = Records.newRecord(StartContainerRequest.class);
    startContainerRequest.setContainerLaunchContext(context);
    try {
      nmClient.startContainer(container, context);
    } catch (YarnException ye) {
      log.error("Received YarnException when starting container: " + container.getId(), ye);
      throw new SamzaContainerLaunchException("Received YarnException when starting container: " + container.getId(), ye);
    } catch (IOException ioe) {
      log.error("Received IOException when starting container: " + container.getId(), ioe);
      throw new SamzaContainerLaunchException("Received IOException when starting container: " + container.getId(), ioe);
    }
  }


  /**
   * @param samzaContainerId  the Samza container Id for logging purposes.
   * @param env               the Map of environment variables to their respective values.
   */
  private void printContainerEnvironmentVariables(int samzaContainerId, Map<String, String> env) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      sb.append(String.format("\n%s=%s", entry.getKey(), entry.getValue()));
    }
    log.info("Container ID {} using environment variables: {}", samzaContainerId, sb.toString());
  }


  /**
   * Gets the environment variables from the specified {@link CommandBuilder} and escapes certain characters.
   *
   * @param cmdBuilder        the command builder containing the environment variables.
   * @return                  the map containing the escaped environment variables.
   */
  private Map<String, String> getEscapedEnvironmentVariablesMap(CommandBuilder cmdBuilder) {
    Map<String, String> env = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : cmdBuilder.buildEnvironment().entrySet()) {
      String escapedValue = Util.envVarEscape(entry.getValue());
      env.put(entry.getKey(), escapedValue);
    }
    return env;
  }


  private String getFormattedCommand(String logDirExpansionVar,
                                     String command,
                                     String stdOut,
                                     String stdErr) {
    return "export SAMZA_LOG_DIR=" + logDirExpansionVar + " && ln -sfn " + logDirExpansionVar +
        " logs && exec ./__package/" + command + " 1>logs/" + stdOut + " 2>logs/" + stdErr;
  }
}
