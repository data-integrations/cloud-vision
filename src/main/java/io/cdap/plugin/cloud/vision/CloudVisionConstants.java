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

/**
 * Cloud Vision constants.
 */
public class CloudVisionConstants {

  protected CloudVisionConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Configuration property name used to specify Google Cloud Project ID, which uniquely identifies a project.
   */
  public static final String PROJECT = "project";

  /**
   * Configuration property name used to specify path on the local file system of the service account key used for
   * authorization. Can be set to {@value AUTO_DETECT} when running on a Dataproc cluster.
   */
  public static final String SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";

  /**
   * {@link CloudVisionConstants#SERVICE_ACCOUNT_FILE_PATH} configuration property used to specify path on the local
   * file system of the service account key used for authorization. Can be set to {@value AUTO_DETECT} when running on
   * a Dataproc cluster.
   */
  public static final String AUTO_DETECT = "auto-detect";

  /**
   * File paths mapped to a record with a single {@value PATH_FIELD_NAME} string field.
   */

  public static final String PATH_FIELD_NAME = "path";

  /**
   * Configuration property name used to specify optional hints.
   */
  public static final String LANGUAGE_HINTS = "languageHints";

  /**
   * Configuration property name used to specify mime type.
   */
  public static final String MIME_TYPE = "mimeType";
}
