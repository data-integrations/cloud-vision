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

import com.google.cloud.vision.v1.AnnotateFileRequest;
import com.google.cloud.vision.v1.AnnotateFileResponse;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.BatchAnnotateFilesResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.GcsSource;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageContext;
import com.google.cloud.vision.v1.InputConfig;
import com.google.protobuf.ByteString;
import io.cdap.plugin.cloud.vision.exception.CloudVisionExecutionException;
import io.cdap.plugin.cloud.vision.transform.CloudVisionClient;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides handy methods to access Google Cloud Vision.
 */
public class DocumentAnnotatorClient extends CloudVisionClient {

  private final DocumentExtractorTransformConfig config;

  public DocumentAnnotatorClient(DocumentExtractorTransformConfig config) {
    super(config);
    this.config = config;
  }

  public AnnotateFileResponse extractDocumentFeature(byte[] content) throws Exception {
    InputConfig inputConfig = InputConfig.newBuilder()
            .setContent(ByteString.copyFrom(content))
            .setMimeType(config.getMimeType())
            .build();
    return extractDocumentFeature(inputConfig);
  }

  public AnnotateFileResponse extractDocumentFeature(String gcsPath) throws Exception {
    InputConfig inputConfig = InputConfig.newBuilder()
            .setGcsSource(GcsSource.newBuilder().setUri(gcsPath))
            .setMimeType(config.getMimeType())
            .build();
    return extractDocumentFeature(inputConfig);
  }

  public AnnotateFileResponse extractDocumentFeature(InputConfig inputConfig) throws Exception {
    try (ImageAnnotatorClient client = createImageAnnotatorClient()) {
      Feature.Type featureType = config.getImageFeature().getFeatureType();
      Feature feature = Feature.newBuilder().setType(featureType).build();

      AnnotateFileRequest.Builder request =
              AnnotateFileRequest.newBuilder()
                      .setInputConfig(inputConfig)
                      .addFeatures(feature)
                      .addAllPages(config.getPagesList());

      ImageContext imageContext = getImageContext();
      if (imageContext != null) {
        request.setImageContext(imageContext);
      }

      BatchAnnotateFilesResponse response = client.batchAnnotateFiles(Collections.singletonList(request.build()));
      AnnotateFileResponse annotateFileResponse = response.getResponses(SINGLE_RESPONSE_INDEX);

      List<String> errors = annotateFileResponse.getResponsesList().stream()
              .filter(AnnotateImageResponse::hasError)
              .map(r -> {
                return String.format("Page '%d' has error: '%s'.",
                        r.getContext().getPageNumber(),
                        r.getError().getMessage());
              })
              .collect(Collectors.toList());

      if (!errors.isEmpty()) {
        String errorMessage = String.format("Unable to extract '%s' feature. %s", featureType,
                String.join(" ", errors));
        throw new CloudVisionExecutionException(errorMessage);
      }

      return annotateFileResponse;
    }
  }
}
