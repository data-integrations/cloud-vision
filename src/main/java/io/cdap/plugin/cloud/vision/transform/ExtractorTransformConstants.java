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

package io.cdap.plugin.cloud.vision.transform;

import io.cdap.plugin.cloud.vision.CloudVisionConstants;

/**
 * Cloud Vision Image Extractor constants.
 */
public class ExtractorTransformConstants extends CloudVisionConstants {

  protected ExtractorTransformConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Configuration property name used to specify field in the input schema containing the path to the image.
   */
  public static final String PATH_FIELD = "pathField";

  /**
   * Configuration property name used to specify field to store the extracted image features.
   */
  public static final String OUTPUT_FIELD = "outputField";

  /**
   * Configuration property name used to specify the features to extract from images.
   */
  public static final String FEATURES = "features";

  /**
   * Configuration property name used to specify hints to detect the language of the text in the images.
   */
  public static final String LANGUAGE_HINTS = "languageHints";

  /**
   * Configuration property name used to specify ratio of the width to the height of the image.
   */
  public static final String ASPECT_RATIOS = "aspectRatios";

  /**
   * Configuration property name used to specify whether to include results derived from the geo information in the
   * image.
   */
  public static final String INCLUDE_GEO_RESULTS = "includeGeoResults";

  /**
   * Configuration property name used to specify resource name of a ProductSet to be searched for similar images.
   */
  public static final String PRODUCT_SET = "productSet";

  /**
   * Configuration property name used to specify a product category to search in.
   */
  public static final String PRODUCT_CATEGORIES = "productCategories";

  /**
   * Configuration property name used to specify bounding polygon for the image detection.
   */
  public static final String BOUNDING_POLYGON = "boundingPolygon";

  /**
   * Configuration property name used to specify a filtering expression. This can be used to restrict search results
   * based on Product labels.
   */
  public static final String FILTER = "filter";

  /**
   * Configuration property name used to specify schema of records output by the transform.
   */
  public static final String SCHEMA = "schema";
}
