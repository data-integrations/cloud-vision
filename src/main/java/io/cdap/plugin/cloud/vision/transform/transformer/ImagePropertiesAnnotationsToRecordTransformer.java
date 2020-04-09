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

import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.ColorInfo;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.ColorInfoSchema;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms image properties annotations of specified {@link AnnotateImageResponse} to {@link StructuredRecord}
 * according to the specified schema.
 */
public class ImagePropertiesAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public ImagePropertiesAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link ColorInfoSchema}. This {@link StructuredRecord} can then be turned into a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    return getOutputRecordBuilder(input)
            .set(outputFieldName, extractDominantColors(annotateImageResponse))
            .build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the dominant colors information from a
   * {@link AnnotateImageResponse} input using a {@link Schema} for the mapping.
   *
   * @param annotateImageResponse The {@link AnnotateImageResponse} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private List<StructuredRecord> extractDominantColors(AnnotateImageResponse annotateImageResponse) {
    return annotateImageResponse.getImagePropertiesAnnotation().getDominantColors().getColorsList().stream()
            .map(this::extractColorInfoRecord)
            .collect(Collectors.toList());
  }

  /**
   * Extract a {@link StructuredRecord} containing the color information from a
   * {@link ColorInfo} input using a {@link Schema} for the mapping.
   *
   * @param colorInfo The {@link ColorInfo} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractColorInfoRecord(ColorInfo colorInfo) {
    Schema faceSchema = getColorInfoSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(faceSchema);

    if (faceSchema.getField(ColorInfoSchema.SCORE_FIELD_NAME) != null) {
      builder.set(ColorInfoSchema.SCORE_FIELD_NAME, colorInfo.getScore());
    }
    if (faceSchema.getField(ColorInfoSchema.PIXEL_FRACTION_FIELD_NAME) != null) {
      builder.set(ColorInfoSchema.PIXEL_FRACTION_FIELD_NAME, colorInfo.getPixelFraction());
    }
    if (faceSchema.getField(ColorInfoSchema.RED_FIELD_NAME) != null) {
      builder.set(ColorInfoSchema.RED_FIELD_NAME, colorInfo.getColor().getRed());
    }
    if (faceSchema.getField(ColorInfoSchema.GREEN_FIELD_NAME) != null) {
      builder.set(ColorInfoSchema.GREEN_FIELD_NAME, colorInfo.getColor().getGreen());
    }
    if (faceSchema.getField(ColorInfoSchema.BLUE_FIELD_NAME) != null) {
      builder.set(ColorInfoSchema.BLUE_FIELD_NAME, colorInfo.getColor().getBlue());
    }
    if (faceSchema.getField(ColorInfoSchema.ALPHA_FIELD_NAME) != null) {
      builder.set(ColorInfoSchema.ALPHA_FIELD_NAME, colorInfo.getColor().getAlpha().getValue());
    }

    return builder.build();
  }

  /**
   * Retrieves Color Info's non-nullable component schema. Color Info's schema retrieved instead of using constant
   * schema since users are free to choose to not include some of the fields
   *
   * @return Color Info's non-nullable component schema.
   */
  private Schema getColorInfoSchema() {
    Schema.Field colorInfoField = schema.getField(outputFieldName);
    return getComponentSchema(colorInfoField);
  }
}
