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
package io.cdap.plugin.cloud.vision.transform.image;

import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfigBuilder;
import javax.annotation.Nullable;

/**
 * Builder class that provides handy methods to construct {@link ExtractorTransformConfig} for testing.
 */
public class ImageExtractorTransformConfigBuilder
  extends ExtractorTransformConfigBuilder<ImageExtractorTransformConfigBuilder> {

  @Nullable
  private String productSet;

  @Nullable
  private String productCategories;

  @Nullable
  private String filter;

  @Nullable
  private String boundingPolygon;

  public static ImageExtractorTransformConfigBuilder builder() {
    return new ImageExtractorTransformConfigBuilder();
  }

  public static ImageExtractorTransformConfigBuilder builder(ImageExtractorTransformConfig original) {
    return new ImageExtractorTransformConfigBuilder()
      .setProject(original.getProject())
      .setServiceFilePath(original.getServiceAccountFilePath())
      .setPathField(original.getPathField())
      .setOutputField(original.getOutputField())
      .setFeatures(original.getFeatures())
      .setLanguageHints(original.getLanguageHints())
      .setAspectRatios(original.getAspectRatios())
      .setIncludeGeoResults(original.isIncludeGeoResults())
      .setProductSet(original.getProductSet())
      .setProductCategories(original.getProductCategories())
      .setBoundingPolygon(original.getBoundingPolygon())
      .setFilter(original.getFilter())
      .setSchema(original.getSchema());
  }

  public ImageExtractorTransformConfigBuilder setProductSet(@Nullable String productSet) {
    this.productSet = productSet;
    return this;
  }

  public ImageExtractorTransformConfigBuilder setProductCategories(@Nullable String productCategories) {
    this.productCategories = productCategories;
    return this;
  }

  public ImageExtractorTransformConfigBuilder setFilter(@Nullable String filter) {
    this.filter = filter;
    return this;
  }

  public ImageExtractorTransformConfigBuilder setBoundingPolygon(@Nullable String boundingPolygon) {
    this.boundingPolygon = boundingPolygon;
    return this;
  }

  @Override
  public ImageExtractorTransformConfig build() {
    return new ImageExtractorTransformConfig(project, serviceFilePath, pathField, outputField, features, languageHints,
      aspectRatios, includeGeoResults, schema, productSet, productCategories, boundingPolygon, filter);
  }
}
