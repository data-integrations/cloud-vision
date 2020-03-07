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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.vision.v1.*;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.cloud.vision.CredentialsHelper;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * Action that runs offline image extractor.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(OfflineImageExtractorAction.NAME)
@Description("Action that runs offline image extractor.")
public class OfflineImageExtractorAction extends Action {
  public static final String NAME = "OfflineImageExtractor";

  private final OfflineImageExtractorActionConfig config;

  public OfflineImageExtractorAction(OfflineImageExtractorActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {
    FailureCollector collector = actionContext.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Credentials credentials = CredentialsHelper.getCredentials(config.getServiceFilePath());

    ImageAnnotatorSettings imageAnnotatorSettings = ImageAnnotatorSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();

    try (ImageAnnotatorClient imageAnnotatorClient = ImageAnnotatorClient.create(imageAnnotatorSettings)) {

      ImageSource source = ImageSource.newBuilder().setImageUri(config.getSourcePath()).build();
      Image image = Image.newBuilder().setSource(source).build();

      List<Feature> features =
              Arrays.asList(Feature.newBuilder().setType(config.getImageFeature().getFeatureType()).build());
      AnnotateImageRequest.Builder builder =
              AnnotateImageRequest.newBuilder().setImage(image).addAllFeatures(features);

      ImageContext imageContext = getImageContext();
      if (imageContext != null) {
        builder.setImageContext(imageContext);
      }

      AnnotateImageRequest requestsElement = builder.build();
      List<AnnotateImageRequest> requests = Arrays.asList(requestsElement);
      GcsDestination gcsDestination = GcsDestination.newBuilder().setUri(config.getDestinationPath()).build();

      // The max number of responses to output in each JSON file
      int batchSize = config.getBatchSizeValue();
      OutputConfig outputConfig =
              OutputConfig.newBuilder()
                      .setGcsDestination(gcsDestination)
                      .setBatchSize(batchSize)
                      .build();

      AsyncBatchAnnotateImagesRequest asyncRequest =
              AsyncBatchAnnotateImagesRequest.newBuilder()
                      .addAllRequests(requests)
                      .setOutputConfig(outputConfig)
                      .build();

      imageAnnotatorClient.asyncBatchAnnotateImagesAsync(asyncRequest)
              .getInitialFuture()
              .get();
    } catch (Exception exception) {
      throw new IllegalStateException(exception);
    }
  }

  @Nullable
  protected ImageContext getImageContext() {
    switch (config.getImageFeature()) {
      case TEXT:
        return Strings.isNullOrEmpty(config.getLanguageHints()) ? null
                : ImageContext.newBuilder().addAllLanguageHints(config.getLanguages()).build();
      case CROP_HINTS:
        CropHintsParams cropHintsParams = CropHintsParams.newBuilder().addAllAspectRatios(config.getAspectRatiosList())
                .build();
        return Strings.isNullOrEmpty(config.getAspectRatios()) ? null
                : ImageContext.newBuilder().setCropHintsParams(cropHintsParams).build();
      case WEB_DETECTION:
        WebDetectionParams webDetectionParams = WebDetectionParams.newBuilder()
                .setIncludeGeoResults(config.getIncludeGeoResults())
                .build();
        return ImageContext.newBuilder().setWebDetectionParams(webDetectionParams).build();
      default:
        return null;
    }
  }

}
