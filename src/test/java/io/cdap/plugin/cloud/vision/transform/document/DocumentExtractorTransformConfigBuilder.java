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

import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfigBuilder;
import javax.annotation.Nullable;

/**
 * Builder class that provides handy methods to construct {@link ExtractorTransformConfig} for testing.
 */
public class DocumentExtractorTransformConfigBuilder
  extends ExtractorTransformConfigBuilder<DocumentExtractorTransformConfigBuilder> {

  @Nullable
  private String contentField;
  private String mimeType;
  private String pages;

  public static DocumentExtractorTransformConfigBuilder builder() {
    return new DocumentExtractorTransformConfigBuilder();
  }

  public static DocumentExtractorTransformConfigBuilder builder(DocumentExtractorTransformConfig original) {
    return new DocumentExtractorTransformConfigBuilder()
      .setProject(original.getProject())
      .setServiceFilePath(original.getServiceAccountFilePath())
      .setPathField(original.getPathField())
      .setOutputField(original.getOutputField())
      .setFeatures(original.getFeatures())
      .setLanguageHints(original.getLanguageHints())
      .setAspectRatios(original.getAspectRatios())
      .setIncludeGeoResults(original.isIncludeGeoResults())
      .setContentField(original.getContentField())
      .setMimeType(original.getMimeType())
      .setPages(original.getPages())
      .setSchema(original.getSchema());
  }

  public DocumentExtractorTransformConfigBuilder setContentField(@Nullable String contentField) {
    this.contentField = contentField;
    return this;
  }

  public DocumentExtractorTransformConfigBuilder setMimeType(String mimeType) {
    this.mimeType = mimeType;
    return this;
  }

  public DocumentExtractorTransformConfigBuilder setPages(String pages) {
    this.pages = pages;
    return this;
  }

  @Override
  public DocumentExtractorTransformConfig build() {
    return new DocumentExtractorTransformConfig(project, serviceFilePath, pathField, outputField, features,
      languageHints, aspectRatios, includeGeoResults, schema, contentField, mimeType, pages);
  }
}
