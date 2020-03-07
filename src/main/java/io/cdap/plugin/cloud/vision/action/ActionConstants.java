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

package io.cdap.plugin.cloud.vision.action;

/**
 * Cloud Vision Action constants.
 */
public final class ActionConstants {

  /**
   * Configuration property name used to specify source path.
   */
  public static final String SOURCE_PATH = "sourcePath";
  /**
   * Configuration property name used to specify destination path.
   */
  public static final String DESTINATION_PATH = "destinationPath";
  /**
   * Configuration property name used to specify batch size.
   */
  public static final String BATCH_SIZE = "batchSize";
  /**
   * Configuration property name used to specify the features to extract
   * from images.
   */
  public static final String FEATURES = "features";
  /**
   * Configuration property name used to specify optional hints.
   */
  public static final String LANGUAGE_HINTS = "languageHints";
  /**
   * Configuration property name used to specify aspect ratios.
   */
  public static final String ASPECT_RATIOS = "aspectRatios";
  /**
   * Configuration property name used to specify includeGeoResults.
   */
  public static final String INCLUDE_GEO_RESULTS = "includeGeoResults";

  /**
   * Prevent instantiation.
   */
  private ActionConstants() {
  }
}
