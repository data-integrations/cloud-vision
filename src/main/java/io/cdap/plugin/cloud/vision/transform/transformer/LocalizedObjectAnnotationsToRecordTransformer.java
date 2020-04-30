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
import com.google.cloud.vision.v1.LocalizedObjectAnnotation;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.LocalizedObjectAnnotationSchema;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms localized object annotations of specified {@link AnnotateImageResponse} to {@link StructuredRecord}
 * according to the specified schema.
 */
public class LocalizedObjectAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public LocalizedObjectAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link io.cdap.plugin.cloud.vision.transform.schema.LocalizedObjectAnnotationSchema}.
   * This {@link StructuredRecord} can then be turned into a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    return getOutputRecordBuilder(input)
      .set(outputFieldName, extractLocalizedObjectAnnotations(annotateImageResponse))
      .build();
  }

  /**
   * Extract a {@link List} of {@link StructuredRecord} containing the localized object information from a
   * {@link AnnotateImageResponse} input using a {@link Schema} for the mapping.
   *
   * @param annotateImageResponse The {@link AnnotateImageResponse} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private List<StructuredRecord> extractLocalizedObjectAnnotations(AnnotateImageResponse annotateImageResponse) {
    return annotateImageResponse.getLocalizedObjectAnnotationsList().stream()
      .map(this::extractLocalizedObjectAnnotationRecord)
      .collect(Collectors.toList());
  }

  /**
   * Extract a {@link StructuredRecord} containing the localized object information from a
   * {@link LocalizedObjectAnnotation} input using a {@link Schema} for the mapping.
   *
   * @param annotation The {@link LocalizedObjectAnnotation} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractLocalizedObjectAnnotationRecord(LocalizedObjectAnnotation annotation) {
    Schema objSchema = getLocalizedObjectAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(objSchema);

    if (objSchema.getField(LocalizedObjectAnnotationSchema.MID_FIELD_NAME) != null) {
      builder.set(LocalizedObjectAnnotationSchema.MID_FIELD_NAME, annotation.getMid());
    }
    if (objSchema.getField(LocalizedObjectAnnotationSchema.LANGUAGE_CODE_FIELD_NAME) != null) {
      String languageCode = annotation.getLanguageCode();
      builder.set(LocalizedObjectAnnotationSchema.LANGUAGE_CODE_FIELD_NAME, languageCode);
    }
    if (objSchema.getField(LocalizedObjectAnnotationSchema.NAME_FIELD_NAME) != null) {
      builder.set(LocalizedObjectAnnotationSchema.NAME_FIELD_NAME, annotation.getName());
    }
    if (objSchema.getField(LocalizedObjectAnnotationSchema.SCORE_FIELD_NAME) != null) {
      builder.set(LocalizedObjectAnnotationSchema.SCORE_FIELD_NAME, annotation.getScore());
    }

    Schema.Field posField = objSchema.getField(LocalizedObjectAnnotationSchema.POSITION_FIELD_NAME);
    if (posField != null) {
      Schema positionSchema = getComponentSchema(posField);
      List<StructuredRecord> position = annotation.getBoundingPoly().getVerticesList().stream()
        .map(v -> extractVertex(v, positionSchema))
        .collect(Collectors.toList());
      builder.set(LocalizedObjectAnnotationSchema.POSITION_FIELD_NAME, position);
    }

    return builder.build();
  }

  /**
   * Retrieves Localized Object Annotation's non-nullable component schema. Schema retrieved instead of using constant
   * schema since users are free to choose to not include some of the fields
   *
   * @return Localized Object Annotation's non-nullable component schema.
   */
  private Schema getLocalizedObjectAnnotationSchema() {
    Schema.Field localizedObjectAnnotationsField = schema.getField(outputFieldName);
    return getComponentSchema(localizedObjectAnnotationsField);
  }
}
