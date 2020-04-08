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
import com.google.cloud.vision.v1.CropHint;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.CropHintAnnotationSchema;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms crop hints annotations of specified {@link AnnotateImageResponse}
 * to {@link StructuredRecord} according to the specified schema.
 */
public class CropHintsAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public CropHintsAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link CropHintAnnotationSchema}. This {@link StructuredRecord} can then be turned into a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  @Override
  public StructuredRecord transform(StructuredRecord input,
                                    AnnotateImageResponse annotateImageResponse) {
    return getOutputRecordBuilder(input)
        .set(outputFieldName, extractCropHintsAnnotations(annotateImageResponse))
        .build();
  }

  private List<StructuredRecord> extractCropHintsAnnotations(AnnotateImageResponse annotateImageResponse) {
    return annotateImageResponse.getCropHintsAnnotation().getCropHintsList().stream()
        .map(this::extractCropHintRecord)
        .collect(Collectors.toList());
  }

  /**
   * Extract a {@link StructuredRecord} from a {@link CropHint} passed in.
   *
   * @param hint Contains the {@link CropHint} information to use.
   * @return {@link StructuredRecord} that contains the data mapped to the {@link Schema}.
   */
  private StructuredRecord extractCropHintRecord(CropHint hint) {
    Schema hintSchema = getCropHintsAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(hintSchema);

    Schema.Field positionField = hintSchema.getField(CropHintAnnotationSchema.POSITION_FIELD_NAME);
    if (positionField != null) {
      Schema positionSchema = getComponentSchema(positionField);
      List<StructuredRecord> position = hint.getBoundingPoly().getVerticesList().stream()
          .map(v -> extractVertex(v, positionSchema))
          .collect(Collectors.toList());
      builder.set(CropHintAnnotationSchema.POSITION_FIELD_NAME, position);
    }
    if (hintSchema.getField(CropHintAnnotationSchema.CONFIDENCE_FIELD_NAME) != null) {
      builder.set(CropHintAnnotationSchema.CONFIDENCE_FIELD_NAME, hint.getConfidence());
    }
    if (hintSchema.getField(CropHintAnnotationSchema.IMPORTANCE_FRACTION_FIELD_NAME) != null) {
      builder.set(CropHintAnnotationSchema.IMPORTANCE_FRACTION_FIELD_NAME,
          hint.getImportanceFraction());
    }

    return builder.build();
  }

  /**
   * Retrieves Crop Hints Annotation's non-nullable component schema. Crop Hints Annotation's schema is retrieved
   * instead of using constant schema since users are free to choose to not include some of the fields.
   *
   * @return Crop Hints Annotation's non-nullable component {@link Schema}.
   */
  private Schema getCropHintsAnnotationSchema() {
    Schema.Field cropHintsAnnotationsField = schema.getField(outputFieldName);
    return getComponentSchema(cropHintsAnnotationsField);
  }
}
