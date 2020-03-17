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
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configuration for OfflineImageExtractorAction.
 */
public class OfflineImageExtractorActionConfig extends PluginConfig {

  @Macro
  @Name(ActionConstants.FEATURES)
  @Description("Features to extract from images.")
  protected final String features;
  @Name(ActionConstants.LANGUAGE_HINTS)
  @Nullable
  @Description("Optional hints to provide to Cloud Vision API.")
  protected final String languageHints;
  @Name(ActionConstants.ASPECT_RATIOS)
  @Nullable
  @Description("Aspect ratios as a decimal number, representing the ratio of the width to the height of the image.")
  protected final String aspectRatios;
  @Name(ActionConstants.INCLUDE_GEO_RESULTS)
  @Nullable
  @Description("Whether to include results derived from the geo information in the image.")
  protected final Boolean includeGeoResults;
  @Macro
  @Name(CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used "
          + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
          + "When running on other clusters, the file must be present on every node in the cluster.")
  @Nullable
  protected String serviceFilePath;
  @Macro
  @Name(ActionConstants.SOURCE_PATH)
  @Description("Path to a source object or directory.")
  private String sourcePath;
  @Macro
  @Name(ActionConstants.DESTINATION_PATH)
  @Description("Path to the destination. The bucket must already exist.")
  private String destinationPath;
  @Macro
  @Name(ActionConstants.BATCH_SIZE)
  @Description("The max number of responses to output in each JSON file.")
  @Nullable
  private String batchSize;

  // Debug
  private static final Logger LOG = LoggerFactory.getLogger(OfflineImageExtractorAction.class);

  public OfflineImageExtractorActionConfig(@Nullable String serviceFilePath, String features,
                                           @Nullable String languageHints, @Nullable String aspectRatios,
                                           @Nullable Boolean includeGeoResults, String sourcePath,
                                           String destinationPath, @Nullable String batchSize) {
    this.serviceFilePath = serviceFilePath;

    if (sourcePath != null) {
      sourcePath = sourcePath.trim();
    }
    this.sourcePath = sourcePath;

    if (destinationPath != null) {
      destinationPath = destinationPath.trim();
    }
    this.destinationPath = destinationPath;

    this.features = features;
    this.batchSize = batchSize;
    this.languageHints = languageHints;
    this.aspectRatios = aspectRatios;
    this.includeGeoResults = includeGeoResults;
  }

  private OfflineImageExtractorActionConfig(Builder builder) {
    serviceFilePath = builder.serviceFilePath;
    sourcePath = builder.sourcePath.trim();
    destinationPath = builder.destinationPath.trim();
    features = builder.features;
    batchSize = builder.batchSize;
    languageHints = builder.languageHints;
    aspectRatios = builder.aspectRatios;
    includeGeoResults = builder.includeGeoResults;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(OfflineImageExtractorActionConfig copy) {
    return builder()
            .setServiceFilePath(copy.getServiceFilePath())
            .setSourcePath(copy.getSourcePath())
            .setDestinationPath(copy.getDestinationPath())
            .setFeatures(copy.getFeatures())
            .setBatchSize(copy.getBatchSize())
            .setLanguageHints(copy.getLanguageHints())
            .setAspectRatios(copy.getAspectRatios())
            .setIncludeGeoResults(copy.getIncludeGeoResults());
  }

  @Nullable
  public String getServiceFilePath() {
    return serviceFilePath;
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public String getDestinationPath() {
    return destinationPath;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public int getBatchSizeValue() {
    return batchSize != null ? Integer.parseInt(batchSize) : 20;
  }

  public String getFeatures() {
    return features;
  }

  public ImageFeature getImageFeature() {
    return Objects.requireNonNull(ImageFeature.fromDisplayName(features));
  }

  @Nullable
  public String getLanguageHints() {
    return languageHints;
  }

  public List<String> getLanguages() {
    return Strings.isNullOrEmpty(languageHints) ? Collections.emptyList() : Arrays.asList(languageHints.split(","));
  }

  @Nullable
  public String getAspectRatios() {
    return aspectRatios;
  }

  public List<Float> getAspectRatiosList() {
    return convertPropertyToList(aspectRatios).stream()
            .map(Float::parseFloat)
            .collect(Collectors.toList());
  }

  public boolean getIncludeGeoResults() {
    return includeGeoResults != null ? includeGeoResults : false;
  }

  private List<String> convertPropertyToList(String property) {
    if (!Strings.isNullOrEmpty(property)) {
      return Arrays.asList(property.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  public void validate(FailureCollector collector) {
    ImageFeature feature = getImageFeature();
    if (feature == null) {
      collector.addFailure(String.format("Incorrect value '%s' for Features.", features), null)
              .withConfigProperty(ActionConstants.FEATURES);
    }

    if (!containsMacro(ActionConstants.BATCH_SIZE) && batchSize != null) {
      int batch;
      try {
        batch = Integer.parseInt(batchSize);
      } catch (NumberFormatException e) {
        collector.addFailure(String.format("Incorrect value '%s' for Batch Size.", batchSize),
                "Provide correct value.")
                .withConfigProperty(ActionConstants.BATCH_SIZE);
        return;
      }

      if (batch < 1 || batch > 100) {
        collector.addFailure("Invalid Batch Size.", "The valid range is [1, 100]")
                .withConfigProperty(ActionConstants.BATCH_SIZE);
      }
    }

    if (ImageFeature.CROP_HINTS.equals(getImageFeature())) {
      convertPropertyToList(aspectRatios).forEach(v -> {
        try {
          Float.parseFloat(v);
        } catch (NumberFormatException e) {
          collector.addFailure(String.format("Incorrect value '%s' for Aspect Ratios.", v), null)
                  .withConfigProperty(ActionConstants.ASPECT_RATIOS);
        }
      });
    }
  }

  public void setSourcePath(String sourcePath) {
    this.sourcePath = sourcePath;
  }

  public void setDestinationPath(String destinationPath) {
    this.destinationPath = destinationPath;
  }

  /**
   * Builder for creating a {@link OfflineImageExtractorActionConfig}.
   */
  public static final class Builder {
    @Nullable
    protected String serviceFilePath;
    protected String features;
    @Nullable
    protected String languageHints;
    @Nullable
    protected String aspectRatios;
    @Nullable
    protected Boolean includeGeoResults;
    private String sourcePath;
    private String destinationPath;
    @Nullable
    private String batchSize;

    private Builder() {
    }

    public Builder setSourcePath(String sourcePath) {
      if (sourcePath != null) {
        this.sourcePath = sourcePath.trim();
      } else {
        this.sourcePath = sourcePath;
      }
      return this;
    }

    public Builder setDestinationPath(String destinationPath) {
      if (destinationPath != null) {
        this.destinationPath = destinationPath.trim();
      } else {
        this.destinationPath = destinationPath;
      }
      return this;
    }

    public Builder setFeatures(String features) {
      this.features = features;
      return this;
    }

    public Builder setBatchSize(String batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setLanguageHints(String languageHints) {
      this.languageHints = languageHints;
      return this;
    }

    public Builder setAspectRatios(String aspectRatios) {
      this.aspectRatios = aspectRatios;
      return this;
    }

    public Builder setServiceFilePath(String serviceFilePath) {
      this.serviceFilePath = serviceFilePath;
      return this;
    }

    public Builder setIncludeGeoResults(Boolean includeGeoResults) {
      this.includeGeoResults = includeGeoResults;
      return this;
    }

    public OfflineImageExtractorActionConfig build() {
      return new OfflineImageExtractorActionConfig(this);
    }
  }
}
