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
import com.google.cloud.vision.v1.AsyncAnnotateFileRequest;
import com.google.cloud.vision.v1.AsyncBatchAnnotateFilesRequest;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.GcsDestination;
import com.google.cloud.vision.v1.GcsSource;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageAnnotatorSettings;
import com.google.cloud.vision.v1.ImageContext;
import com.google.cloud.vision.v1.InputConfig;
import com.google.cloud.vision.v1.OutputConfig;
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

import java.util.ArrayList;
import java.util.List;

import static io.cdap.plugin.cloud.vision.action.ActionConstants.MAX_NUMBER_OF_IMAGES_PER_BATCH;

/**
 * Action that runs offline document text extractor.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(OfflineTextExtractorAction.PLUGIN_NAME)
@Description("Action that runs offline document text extractor")
public class OfflineTextExtractorAction extends Action {
  public static final String PLUGIN_NAME = "OfflineTextExtractor";
  private final OfflineTextExtractorActionConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(OfflineTextExtractorAction.class);

  public OfflineTextExtractorAction(OfflineTextExtractorActionConfig config) {
    if (config.getSourcePath() != null) {
      config.setSourcePath(config.getSourcePath().trim()); // Remove whitespace
    }
    if (config.getDestinationPath() != null) {
      config.setDestinationPath(config.getDestinationPath().trim());
    }
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {
    FailureCollector collector = actionContext.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Credentials credentials = CredentialsHelper.getCredentials(config.getServiceAccountFilePath());

    // Destination in GCS where the results will be stored
    String destinationPath = config.getDestinationPath();
    // Add a / at the end if it's not already there
    if (!destinationPath.endsWith("/")) {
      destinationPath += "/";
    }
    GcsDestination gcsDestination = GcsDestination.newBuilder()
            .setUri(destinationPath)
            .build();

    LOG.info("Setting destination path to: " + destinationPath);

    OutputConfig outputConfig = OutputConfig.newBuilder()
            .setBatchSize(config.getBatchSize())
            .setGcsDestination(gcsDestination)
            .build();

    ImageAnnotatorSettings imageAnnotatorSettings = ImageAnnotatorSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();

    // Get all the blobs in the source path
    List<Blob> blobs = GcsBucketHelper.getAllFilesInPath(config.getSourcePath(), credentials);
    if (blobs.isEmpty()) {
      LOG.warn("Nothing found to process in path: " + config.getSourcePath());
      return;
    }

    // Prepare the list of requests
    List<AsyncAnnotateFileRequest> requests = new ArrayList<>(blobs.size());

    // Feature we are going to ask for
    Feature feature = Feature.newBuilder()
            .setType(Feature.Type.DOCUMENT_TEXT_DETECTION)
            .build();

    try (ImageAnnotatorClient client = ImageAnnotatorClient.create(imageAnnotatorSettings)) {

      // Create batches of images to send for processing
      for (int batchId = 0;
           batchId < (1 + blobs.size() / MAX_NUMBER_OF_IMAGES_PER_BATCH);
           batchId++) {
        for (int index = batchId * MAX_NUMBER_OF_IMAGES_PER_BATCH;
             (index < (batchId + 1) * MAX_NUMBER_OF_IMAGES_PER_BATCH) && (index < blobs.size());
             index++) {
          Blob blob = blobs.get(index);
          // Rebuild the full path of the blob
          String fullBlobPath = "gs://" + blob.getBucket() + "/" + blob.getName();
          LOG.info("Adding blob: " + fullBlobPath + " to the list of requests");

          GcsSource gcsSource = GcsSource.newBuilder()
                  .setUri(fullBlobPath)
                  .build();

          InputConfig inputConfig = InputConfig.newBuilder()
                  .setMimeType(config.getMimeType())
                  .setGcsSource(gcsSource)
                  .build();

          ImageContext context = ImageContext.newBuilder()
                  .addAllLanguageHints(config.getLanguageHintsList())
                  .build();

          AsyncAnnotateFileRequest request = AsyncAnnotateFileRequest.newBuilder()
                  .addFeatures(feature)
                  .setImageContext(context)
                  .setInputConfig(inputConfig)
                  .setOutputConfig(outputConfig)
                  .build();

          requests.add(request);
        }

        // Send the requests
        AsyncBatchAnnotateFilesRequest asyncRequest = AsyncBatchAnnotateFilesRequest.newBuilder()
                .addAllRequests(requests)
                .build();

        // Wait for the future to complete
        client.asyncBatchAnnotateFilesAsync(asyncRequest)
                .getInitialFuture()
                .get();
      }
    } catch (Exception exception) {
      throw new IllegalStateException(exception);
    }
  }
}
