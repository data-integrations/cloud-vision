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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.cloud.vision.CloudVisionConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Config class for {@link OfflineTextExtractorAction}.
 */
public class OfflineTextExtractorActionConfig extends PluginConfig {

  @Name(ActionConstants.SOURCE_PATH)
  @Macro
  @Description("Path to the location of the directory on GCS where the input files are stored.")
  private String sourcePath;
  @Name(ActionConstants.DESTINATION_PATH)
  @Macro
  @Description("Path to the location of the directory on GCS where output files should be stored.")
  private String destinationPath;
  @Name(CloudVisionConstants.MIME_TYPE)
  @Description("Document type.")
  private final String mimeType;
  @Name(ActionConstants.BATCH_SIZE)
  @Description("The max number of responses.")
  private final Integer batchSize;
  @Name(CloudVisionConstants.LANGUAGE_HINTS)
  @Nullable
  @Description("Optional hints to provide to Cloud Vision API.")
  private final String languageHints;
  @Name(CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used "
      + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
      + "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  private String serviceFilePath;

  public OfflineTextExtractorActionConfig(String serviceFilePath, String sourcePath,
                                          String destinationPath, String mimeType,
                                          Integer batchSize, @Nullable String languageHints) {
    this.serviceFilePath = serviceFilePath;
    this.sourcePath = sourcePath;
    this.destinationPath = destinationPath;
    this.mimeType = mimeType;
    this.batchSize = batchSize;
    this.languageHints = languageHints;
  }

  private OfflineTextExtractorActionConfig(Builder builder) {
    serviceFilePath = builder.serviceFilePath;
    sourcePath = builder.sourcePath;
    destinationPath = builder.destinationPath;
    mimeType = builder.mimeType;
    batchSize = builder.batchSize;
    languageHints = builder.languageHints;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Helper function to get a Builder object based on an existing configuration.
   *
   * @param copy Configuration object to use as the source to copy from.
   * @return Builer object.
   */
  public static Builder builder(OfflineTextExtractorActionConfig copy) {
    Builder builder = new Builder();

    builder.setServiceFilePath(copy.getServiceFilePath());
    builder.setSourcePath(copy.getSourcePath());
    builder.setDestinationPath(copy.getDestinationPath());
    builder.setMimeType(copy.getMimeType());
    builder.setBatchSize(copy.getBatchSize());
    builder.setLanguageHints(copy.getLanguageHints());

    return builder;
  }

  public String getServiceFilePath() {
    return serviceFilePath;
  }

  @Nullable
  public String getServiceAccountFilePath() {
    if (containsMacro(CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH)
        || Strings.isNullOrEmpty(serviceFilePath)) {
      return null;
    }

    return serviceFilePath;
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public String getDestinationPath() {
    return destinationPath;
  }

  public String getMimeType() {
    return mimeType;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  @Nullable
  public String getLanguageHints() {
    return languageHints;
  }

  public List<String> getLanguageHintsList() {
    if (!Strings.isNullOrEmpty(languageHints)) {
      return Arrays.asList(languageHints.split(","));
    }

    return Collections.emptyList();
  }

  public void setSourcePath(String sourcePath) {
    this.sourcePath = sourcePath;
  }

  public void setDestinationPath(String destinationPath) {
    this.destinationPath = destinationPath;
  }

  /**
   * Validate that the configuration is correct. If not, use the FailureCollector object passed to report errors.
   *
   * @param collector FailureCollector object to use to report errors.
   */
  public void validate(FailureCollector collector) {
    if (!containsMacro(CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH)
        && Strings.isNullOrEmpty(serviceFilePath)) {
      collector.addFailure("Service account file path must be specified.", null)
          .withConfigProperty(CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH);
    }

    if (!containsMacro(ActionConstants.SOURCE_PATH) && Strings.isNullOrEmpty(sourcePath)) {
      collector.addFailure("Source path must be specified.", null)
          .withConfigProperty(ActionConstants.SOURCE_PATH);
    }

    if (!containsMacro(ActionConstants.DESTINATION_PATH) && Strings.isNullOrEmpty(destinationPath)) {
      collector.addFailure("Destination path must be specified.", null)
          .withConfigProperty(ActionConstants.DESTINATION_PATH);
    }
  }

  /**
   * Builder for creating a {@link OfflineTextExtractorActionConfig}.
   */
  public static final class Builder {
    private String serviceFilePath;
    private String sourcePath;
    private String destinationPath;
    private String mimeType;
    private Integer batchSize;

    @Nullable
    private String languageHints;

    private Builder() {
    }

    public Builder setServiceFilePath(String serviceFilePath) {
      this.serviceFilePath = serviceFilePath;
      return this;
    }

    public Builder setSourcePath(String sourcePath) {
      this.sourcePath = sourcePath;
      return this;
    }

    public Builder setDestinationPath(String destinationPath) {
      this.destinationPath = destinationPath;
      return this;
    }

    public Builder setMimeType(String mimeType) {
      this.mimeType = mimeType;
      return this;
    }

    public Builder setBatchSize(Integer batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setLanguageHints(@Nullable String languageHints) {
      this.languageHints = languageHints;
      return this;
    }

    public OfflineTextExtractorActionConfig build() {
      return new OfflineTextExtractorActionConfig(this);
    }
  }
}
