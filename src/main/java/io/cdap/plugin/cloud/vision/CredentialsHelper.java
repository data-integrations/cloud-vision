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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Helper to provide credentials and project id.
 */
public final class CredentialsHelper {

  public static ServiceAccountCredentials getCredentials(String path) throws IOException {
    if (CloudVisionConstants.AUTO_DETECT.equals(path)) {
      return (ServiceAccountCredentials) GoogleCredentials.getApplicationDefault();
    }

    File credentialsPath = new File(path);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
  }

  public static String getProjectId(String project) {
    if (Strings.isNullOrEmpty(project) || CloudVisionConstants.AUTO_DETECT.equals(project)) {
      return ServiceOptions.getDefaultProjectId();
    }

    return project;
  }
}
