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

import javax.annotation.Nullable;

/**
 * Builder class that provides handy methods to construct {@link CloudVisionConfig} for testing.
 */
public abstract class CloudVisionConfigBuilder<T extends CloudVisionConfigBuilder> {

  @Nullable
  protected String project;

  @Nullable
  protected String serviceFilePath;

  public T setProject(@Nullable String project) {
    this.project = project;
    return (T) this;
  }

  public T setServiceFilePath(@Nullable String serviceFilePath) {
    this.serviceFilePath = serviceFilePath;
    return (T) this;
  }

  public abstract CloudVisionConfig build();
}
