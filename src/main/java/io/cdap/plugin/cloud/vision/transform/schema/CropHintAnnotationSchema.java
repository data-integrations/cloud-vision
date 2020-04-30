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

package io.cdap.plugin.cloud.vision.transform.schema;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Single crop hint that is used to generate a new crop when serving an image.
 * {@link com.google.cloud.vision.v1.CropHint} mapped to a record with following fields.
 */
public class CropHintAnnotationSchema {

  private CropHintAnnotationSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * The bounding polygon for the crop region. The coordinates of the bounding box are in the original image's scale.
   */
  public static final String POSITION_FIELD_NAME = "position";

  /**
   * Confidence of this being a salient region.  Range [0, 1].
   */
  public static final String CONFIDENCE_FIELD_NAME = "confidence";

  /**
   * Fraction of importance of this salient region with respect to the original image.
   */
  public static final String IMPORTANCE_FRACTION_FIELD_NAME = "importanceFraction";

  public static final Schema SCHEMA = Schema.recordOf(
    "crop-hint-annotation-component-record",
    Schema.Field.of(POSITION_FIELD_NAME, Schema.arrayOf(VertexSchema.getSchema("cropHintAnnotation-position"))),
    Schema.Field.of(CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(IMPORTANCE_FRACTION_FIELD_NAME, Schema.of(Schema.Type.FLOAT)));

}
