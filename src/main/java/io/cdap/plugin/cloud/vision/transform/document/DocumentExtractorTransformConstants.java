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

package io.cdap.plugin.cloud.vision.transform.document;

import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConstants;

/**
 * Cloud Vision Image Extractor constants.
 */
public class DocumentExtractorTransformConstants extends ExtractorTransformConstants {

  private DocumentExtractorTransformConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Configuration property name used to specify field in the input schema containing the file content, represented as
   * a stream of bytes.
   */
  public static final String CONTENT_FIELD = "contentField";

  /**
   * Configuration property name used to specify the type of the file. Currently only 'application/pdf', 'image/tiff'
   * and 'image/gif' are supported. Wildcards are not supported.
   */
  public static final String MIME_TYPE = "mimeType";

  /**
   * Configuration property name used to specify the pages in the file to perform image annotation.
   */
  public static final String PAGES = "pages";

  /**
   * File Annotation mapped to record with field {@value PAGE_FIELD_NAME} for page number and
   * {@value FEATURE_FIELD_NAME} field for extracted image feature.
   */
  public static final String PAGE_FIELD_NAME = "page";

  /**
   * File Annotation mapped to record with field {@value PAGE_FIELD_NAME} for page number and
   * {@value FEATURE_FIELD_NAME} field for extracted image feature.
   */
  public static final String FEATURE_FIELD_NAME = "feature";
}
