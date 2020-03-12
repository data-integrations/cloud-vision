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
import com.google.cloud.storage.Blob;
import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.AsyncBatchAnnotateImagesRequest;
import com.google.cloud.vision.v1.CropHintsParams;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.GcsDestination;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageAnnotatorSettings;
import com.google.cloud.vision.v1.ImageContext;
import com.google.cloud.vision.v1.ImageSource;
import com.google.cloud.vision.v1.OutputConfig;
import com.google.cloud.vision.v1.WebDetectionParams;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.cloud.vision.CredentialsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.cdap.plugin.cloud.vision.action.ActionConstants.MAX_NUMBER_OF_IMAGES_PER_BATCH;

/**
 * Action that runs offline image extractor.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(OfflineImageExtractorAction.NAME)
@Description("Action that runs offline image extractor.")
public class OfflineImageExtractorAction extends Action {
  public static final String NAME = "OfflineImageExtractor";

  private final OfflineImageExtractorActionConfig config;

  // Logging
  private static Logger LOG = LoggerFactory.getLogger(OfflineImageExtractorAction.class);

  public OfflineImageExtractorAction(OfflineImageExtractorActionConfig config) {
    if (config.getSourcePath() != null)
      config.setSourcePath(config.getSourcePath().trim()); // Remove whitespace

    if (config.getDestinationPath() != null)
      config.setDestinationPath(config.getDestinationPath().trim());

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

    // The max number of responses to output in each JSON file
    int batchSize = config.getBatchSizeValue();

    Credentials credentials = CredentialsHelper.getCredentials(config.getServiceFilePath());

    // Destination in GCS where the results will be stored
    String destinationPath = config.getDestinationPath();
    // Add a / at the end if it's not already there
    if (!destinationPath.endsWith("/"))
      destinationPath += "/";
    GcsDestination gcsDestination = GcsDestination.newBuilder().setUri(destinationPath).build();

    LOG.warn("Setting destination path to: " + destinationPath);

    OutputConfig outputConfig =
            OutputConfig.newBuilder()
                    .setGcsDestination(gcsDestination)
                    .setBatchSize(batchSize)
                    .build();

    ImageAnnotatorSettings imageAnnotatorSettings = ImageAnnotatorSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();

    // Get all the blobs in the source path
    List<Blob> blobs = GcsBucketHelper.getAllFilesInPath(config.getSourcePath(), credentials);

    // Prepare the list of requests
    List<AnnotateImageRequest> imageRequests = new ArrayList<>(blobs.size());

    // Features we are going to ask for (only one)
    List<Feature> features = Arrays.asList(Feature.newBuilder()
            .setType(config.getImageFeature().getFeatureType())
            .build());

    try (ImageAnnotatorClient imageAnnotatorClient = ImageAnnotatorClient.create(imageAnnotatorSettings)) {

      // Create batches of images to send for processing
      for (int batch_id = 0;
           batch_id < (1 + blobs.size() / MAX_NUMBER_OF_IMAGES_PER_BATCH);
           batch_id++) {
        for (int index = batch_id * MAX_NUMBER_OF_IMAGES_PER_BATCH;
             (index < (batch_id + 1) * MAX_NUMBER_OF_IMAGES_PER_BATCH) && (index < blobs.size());
             index++) {
          Blob blob = blobs.get(index);
          // Rebuild the full path of the blob
          String fullBlobPath = "gs://" + blob.getBucket() + "/" + blob.getName();

          LOG.info("Adding blob: " + fullBlobPath + " to the list of requests");

          ImageSource imageSource = ImageSource.newBuilder().setImageUri(fullBlobPath).build();
          Image image = Image.newBuilder().setSource(imageSource).build();

          AnnotateImageRequest.Builder builder =
                  AnnotateImageRequest.newBuilder()
                          .setImage(image)
                          .addAllFeatures(features);

          ImageContext imageContext = getImageContext();
          if (imageContext != null) {
            builder.setImageContext(imageContext);
          }

          AnnotateImageRequest annotateImageRequest = builder.build();
          imageRequests.add(annotateImageRequest);
        }

        // Send the requests
        AsyncBatchAnnotateImagesRequest asyncRequest =
                AsyncBatchAnnotateImagesRequest.newBuilder()
                        .addAllRequests(imageRequests)
                        .setOutputConfig(outputConfig)
                        .build();

        // Wait for the future to complete
        imageAnnotatorClient.asyncBatchAnnotateImagesAsync(asyncRequest)
                .getInitialFuture()
                .get();
      }
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
