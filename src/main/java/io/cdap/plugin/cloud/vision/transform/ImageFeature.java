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

import com.google.cloud.vision.v1.Feature;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.ColorInfoSchema;
import io.cdap.plugin.cloud.vision.transform.schema.CropHintAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.EntityAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.EntityAnnotationWithPositionSchema;
import io.cdap.plugin.cloud.vision.transform.schema.FaceAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.FullTextAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.LocalizedObjectAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.ProductSearchResultsSchema;
import io.cdap.plugin.cloud.vision.transform.schema.SafeSearchAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.TextAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.WebDetectionSchema;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The features to extract from images.
 */
public enum ImageFeature {

  FACE("Face", Feature.Type.FACE_DETECTION, Schema.arrayOf(FaceAnnotationSchema.SCHEMA)),
  TEXT("Text", Feature.Type.TEXT_DETECTION, Schema.arrayOf(TextAnnotationSchema.SCHEMA)),
  CROP_HINTS("Crop Hints", Feature.Type.CROP_HINTS, Schema.arrayOf(CropHintAnnotationSchema.SCHEMA)),
  HANDWRITING("Handwriting", Feature.Type.DOCUMENT_TEXT_DETECTION, FullTextAnnotationSchema.SCHEMA),
  IMAGE_PROPERTIES("Image Properties", Feature.Type.IMAGE_PROPERTIES, Schema.arrayOf(ColorInfoSchema.SCHEMA)),
  LABELS("Labels", Feature.Type.LABEL_DETECTION, Schema.arrayOf(EntityAnnotationSchema.SCHEMA)),
  LANDMARKS("Landmarks", Feature.Type.LANDMARK_DETECTION, Schema.arrayOf(EntityAnnotationWithPositionSchema.SCHEMA)),
  LOGOS("Logos", Feature.Type.LOGO_DETECTION, Schema.arrayOf(EntityAnnotationWithPositionSchema.SCHEMA)),
  // Object localization is used to detect multiple objects
  EXPLICIT_CONTENT("Explicit Content", Feature.Type.SAFE_SEARCH_DETECTION, SafeSearchAnnotationSchema.SCHEMA),
  WEB_DETECTION("Web Detection", Feature.Type.WEB_DETECTION, WebDetectionSchema.SCHEMA),
  PRODUCT_SEARCH("Product Search", Feature.Type.PRODUCT_SEARCH, ProductSearchResultsSchema.SCHEMA),
  OBJECT_LOCALIZATION(
    "Object Localization",
    Feature.Type.OBJECT_LOCALIZATION,
    Schema.arrayOf(LocalizedObjectAnnotationSchema.SCHEMA));

  private static final Map<String, ImageFeature> byDisplayName = Arrays.stream(values())
    .collect(Collectors.toMap(ImageFeature::getDisplayName, Function.identity()));

  private final String displayName;
  private final Feature.Type featureType;
  private final Schema schema;

  ImageFeature(String displayName, Feature.Type featureType, Schema schema) {
    this.displayName = displayName;
    this.featureType = featureType;
    this.schema = schema;
  }

  @Nullable
  public static ImageFeature fromDisplayName(String displayName) {
    return byDisplayName.get(displayName);
  }

  public String getDisplayName() {
    return displayName;
  }

  public Feature.Type getFeatureType() {
    return featureType;
  }

  public Schema getSchema() {
    return schema;
  }
}
