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

import io.cdap.plugin.cloud.vision.CloudVisionConfigBuilder;
import javax.annotation.Nullable;

/**
 * Builder class that provides handy methods to construct {@link ExtractorTransformConfig} for testing.
 */
public abstract class ExtractorTransformConfigBuilder<T extends ExtractorTransformConfigBuilder>
        extends CloudVisionConfigBuilder<T> {

  protected String pathField;
  protected String outputField;
  protected String features;

  @Nullable
  protected Boolean includeGeoResults;

  @Nullable
  protected String languageHints;

  @Nullable
  protected String aspectRatios;

  @Nullable
  protected String schema;

  public T setPathField(String pathField) {
    this.pathField = pathField;
    return (T) this;
  }

  public T setOutputField(String outputField) {
    this.outputField = outputField;
    return (T) this;
  }

  public T setFeatures(String features) {
    this.features = features;
    return (T) this;
  }

  public T setLanguageHints(@Nullable String languageHints) {
    this.languageHints = languageHints;
    return (T) this;
  }

  public T setSchema(@Nullable String schema) {
    this.schema = schema;
    return (T) this;
  }

  public T setIncludeGeoResults(@Nullable Boolean includeGeoResults) {
    this.includeGeoResults = includeGeoResults;
    return (T) this;
  }

  public T setAspectRatios(@Nullable String aspectRatios) {
    this.aspectRatios = aspectRatios;
    return (T) this;
  }

  public abstract ExtractorTransformConfig build();
}
