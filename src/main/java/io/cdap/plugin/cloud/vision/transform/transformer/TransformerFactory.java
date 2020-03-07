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
package io.cdap.plugin.cloud.vision.transform.transformer;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;

/**
 * A factory which creates instance of {@ImageAnnotationToRecordTransformer} in accordance to feature and output schema
 * configured in input config.
 */
public class TransformerFactory {
  public static ImageAnnotationToRecordTransformer createInstance(ImageFeature imageFeature, String outputFieldName,
                                                                  Schema schema) {
    switch (imageFeature) {
      case FACE:
        return new FaceAnnotationsToRecordTransformer(schema, outputFieldName);
      case TEXT:
        return new TextAnnotationsToRecordTransformer(schema, outputFieldName);
      case HANDWRITING:
        return new FullTextAnnotationsToRecordTransformer(schema, outputFieldName);
      case CROP_HINTS:
        return new CropHintsAnnotationsToRecordTransformer(schema, outputFieldName);
      case IMAGE_PROPERTIES:
        return new ImagePropertiesAnnotationsToRecordTransformer(schema, outputFieldName);
      case LABELS:
        return new LabelAnnotationsToRecordTransformer(schema, outputFieldName);
      case LANDMARKS:
        return new LandmarkAnnotationsToRecordTransformer(schema, outputFieldName);
      case LOGOS:
        return new LogoAnnotationsToRecordTransformer(schema, outputFieldName);
      case OBJECT_LOCALIZATION:
        return new LocalizedObjectAnnotationsToRecordTransformer(schema, outputFieldName);
      case EXPLICIT_CONTENT:
        return new SafeSearchAnnotationsToRecordTransformer(schema, outputFieldName);
      case WEB_DETECTION:
        return new WebDetectionToRecordTransformer(schema, outputFieldName);
      case PRODUCT_SEARCH:
        return new ProductSearchResultToRecordTransformer(schema, outputFieldName);
      default:
        throw new IllegalArgumentException(String.format("Unsupported image feature: '%s'", imageFeature));
    }
  }
}
