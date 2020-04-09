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

import com.google.api.client.util.Strings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.vision.v1.CropHintsParams;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageAnnotatorSettings;
import com.google.cloud.vision.v1.ImageContext;
import com.google.cloud.vision.v1.WebDetectionParams;
import io.cdap.plugin.cloud.vision.CredentialsHelper;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Provides handy methods to access Google Cloud Vision.
 */
public abstract class CloudVisionClient {

  protected static final int SINGLE_RESPONSE_INDEX = 0;

  private final ExtractorTransformConfig config;

  public CloudVisionClient(ExtractorTransformConfig config) {
    this.config = config;
  }

  protected ImageAnnotatorClient createImageAnnotatorClient() throws IOException {
    String serviceAccountPath = config.getServiceAccountFilePath();
    Credentials credentials = serviceAccountPath == null ? null : CredentialsHelper.getCredentials(serviceAccountPath);
    ImageAnnotatorSettings.Builder imageAnnotatorSettings = ImageAnnotatorSettings.newBuilder();
    if (credentials != null) {
      imageAnnotatorSettings.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }
    return ImageAnnotatorClient.create(imageAnnotatorSettings.build());
  }

  @Nullable
  protected ImageContext getImageContext() {
    switch (config.getImageFeature()) {
      case TEXT:
        return Strings.isNullOrEmpty(config.getLanguageHints()) ? null
          : ImageContext.newBuilder().addAllLanguageHints(config.getLanguages()).build();
      case CROP_HINTS:
        if (Strings.isNullOrEmpty(config.getAspectRatios())) {
          return null;
        }
        CropHintsParams cropHintsParams = CropHintsParams.newBuilder()
          .addAllAspectRatios(config.getAspectRatiosList())
          .build();
        return ImageContext.newBuilder().setCropHintsParams(cropHintsParams).build();
      case WEB_DETECTION:
        WebDetectionParams webDetectionParams = WebDetectionParams.newBuilder()
          .setIncludeGeoResults(config.isIncludeGeoResults())
          .build();
        return ImageContext.newBuilder().setWebDetectionParams(webDetectionParams).build();
      default:
        return null;
    }
  }
}
