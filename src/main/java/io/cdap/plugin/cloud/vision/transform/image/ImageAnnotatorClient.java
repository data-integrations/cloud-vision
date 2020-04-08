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

package io.cdap.plugin.cloud.vision.transform.image;

import com.google.api.client.util.Strings;
import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.BatchAnnotateImagesResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageContext;
import com.google.cloud.vision.v1.ImageSource;
import com.google.cloud.vision.v1.ProductSearchParams;
import io.cdap.plugin.cloud.vision.exception.CloudVisionExecutionException;
import io.cdap.plugin.cloud.vision.transform.CloudVisionClient;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Provides handy methods to access Google Cloud Vision.
 */
public class ImageAnnotatorClient extends CloudVisionClient {

  private final ImageExtractorTransformConfig config;

  public ImageAnnotatorClient(ImageExtractorTransformConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * @param gcsPath {@link String} that contains the path to a blon in GCS to use with the Cloud vision API.
   * @return {@link AnnotateImageResponse} with the information requested from the cloud vision API.
   * @throws Exception if there was an error sent back by the cloud vision API.
   */
  public AnnotateImageResponse extractImageFeature(String gcsPath) throws Exception {
    try (com.google.cloud.vision.v1.ImageAnnotatorClient client = createImageAnnotatorClient()) {
      ImageSource imgSource = ImageSource.newBuilder().setGcsImageUri(gcsPath).build();
      Image img = Image.newBuilder().setSource(imgSource).build();

      Feature.Type featureType = config.getImageFeature().getFeatureType();
      Feature feature = Feature.newBuilder().setType(featureType).build();

      AnnotateImageRequest.Builder request = AnnotateImageRequest
          .newBuilder().addFeatures(feature).setImage(img);
      ImageContext imageContext = getImageContext();
      if (imageContext != null) {
        request.setImageContext(imageContext);
      }

      BatchAnnotateImagesResponse response = client
          .batchAnnotateImages(Collections.singletonList(request.build()));

      AnnotateImageResponse annotateImageResponse = response.getResponses(SINGLE_RESPONSE_INDEX);
      if (annotateImageResponse.hasError()) {
        String errorMessage = String.format("Unable to extract '%s' feature of image '%s' due to: '%s'", featureType,
            gcsPath, annotateImageResponse.getError().getMessage());
        throw new CloudVisionExecutionException(errorMessage);
      }
      return annotateImageResponse;
    }
  }

  @Override
  @Nullable
  protected ImageContext getImageContext() {
    if (config.getImageFeature() != ImageFeature.PRODUCT_SEARCH) {
      return super.getImageContext();
    }
    // Product Search parameters
    ProductSearchParams.Builder productSearchParams = ProductSearchParams.newBuilder()
        .setProductSet(config.getProductSet())
        .addProductCategories(config.getProductCategory().getName());
    if (!Strings.isNullOrEmpty(config.getFilter())) {
      productSearchParams.setFilter(config.getFilter());
    }
    if (!Strings.isNullOrEmpty(config.getBoundingPolygon())) {
      productSearchParams.setBoundingPoly(config.getBoundingPoly());
    }

    return ImageContext.newBuilder().setProductSearchParams(productSearchParams).build();
  }
}
