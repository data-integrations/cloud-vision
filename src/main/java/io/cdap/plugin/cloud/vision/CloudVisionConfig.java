/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.cloud.vision;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Cloud Vision plugins can use.
 */
public class CloudVisionConfig extends PluginConfig {

  @Name(CloudVisionConstants.PROJECT)
  @Description("Google Cloud Project ID, which uniquely identifies a project. "
    + "It can be found on the Dashboard in the Google Cloud Platform Console.")
  @Macro
  @Nullable
  protected String project;

  @Name(CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used "
    + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
    + "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  @Nullable
  protected String serviceFilePath;

  public CloudVisionConfig(String project, String serviceFilePath) {
    this.project = project;
    this.serviceFilePath = serviceFilePath;
  }

  @Nullable
  public String getProject() {
    return project;
  }

  @Nullable
  public String getServiceFilePath() {
    return serviceFilePath;
  }

  @Nullable
  public String getServiceAccountFilePath() {
    if (containsMacro(CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH) || Strings.isNullOrEmpty(serviceFilePath)
      || CloudVisionConstants.AUTO_DETECT.equals(serviceFilePath)) {
      return null;
    }
    return serviceFilePath;
  }
}
