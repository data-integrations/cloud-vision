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

/**
 * Action that runs offline document text extractor.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(TextExtractorAction.PLUGIN_NAME)
@Description("Action that runs offline document text extractor")
public class TextExtractorAction extends Action {
  public static final String PLUGIN_NAME = "TextExtractorOffline";
  private static final Logger LOG = LoggerFactory.getLogger(TextExtractorAction.class);

  private final TextExtractorActionConfig config;

  public TextExtractorAction(TextExtractorActionConfig config) {
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
    ImageAnnotatorSettings imageAnnotatorSettings = ImageAnnotatorSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();

    try (ImageAnnotatorClient client = ImageAnnotatorClient.create(imageAnnotatorSettings)) {
      List<AsyncAnnotateFileRequest> requests = new ArrayList<>();

      GcsSource gcsSource = GcsSource.newBuilder()
              .setUri(config.getSourcePath())
              .build();

      GcsDestination gcsDestination = GcsDestination.newBuilder()
              .setUri(config.getDestinationPath())
              .build();

      InputConfig inputConfig = InputConfig.newBuilder()
              .setMimeType(config.getMimeType())
              .setGcsSource(gcsSource)
              .build();

      OutputConfig outputConfig = OutputConfig.newBuilder()
              .setBatchSize(config.getBatchSize())
              .setGcsDestination(gcsDestination)
              .build();

      Feature feature = Feature.newBuilder()
              .setType(Feature.Type.DOCUMENT_TEXT_DETECTION)
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
      AsyncBatchAnnotateFilesRequest asyncRequest = AsyncBatchAnnotateFilesRequest.newBuilder()
              .addAllRequests(requests)
              .build();

      client.asyncBatchAnnotateFilesAsync(asyncRequest)
              .getInitialFuture()
              .get();
    }
  }
}
