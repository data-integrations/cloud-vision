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
 * Color information consists of RGB channels, score, and the fraction of the image that the color occupies in the
 * image.
 */
public class ColorInfoSchema {

  private ColorInfoSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * The fraction of pixels the color occupies in the image. Value in range [0, 1].
   */
  public static final String PIXEL_FRACTION_FIELD_NAME = "pixelFraction";

  /**
   * Image-specific score for this color. Value in range [0, 1].
   */
  public static final String SCORE_FIELD_NAME = "score";

  /**
   * The amount of red in the color as a value in the interval [0, 1].
   */
  public static final String RED_FIELD_NAME = "red";

  /**
   * The amount of green in the color as a value in the interval [0, 1].
   */
  public static final String GREEN_FIELD_NAME = "green";

  /**
   * The amount of blue in the color as a value in the interval [0, 1].
   */
  public static final String BLUE_FIELD_NAME = "blue";

  /**
   * The fraction of this color that should be applied to the pixel.
   */
  public static final String ALPHA_FIELD_NAME = "alpha";

  public static final Schema SCHEMA = Schema.recordOf(
    "dominant-colors-annotation-component-record",
    Schema.Field.of(PIXEL_FRACTION_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(RED_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(GREEN_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(BLUE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(ALPHA_FIELD_NAME, Schema.of(Schema.Type.FLOAT)));
}
