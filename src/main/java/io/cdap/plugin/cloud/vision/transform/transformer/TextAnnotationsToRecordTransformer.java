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
import com.google.cloud.vision.v1.EntityAnnotation;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.TextAnnotationSchema;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms text annotations of specified {@link AnnotateImageResponse} to {@link StructuredRecord} according to
 * the specified schema.
 */
public class TextAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public TextAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link TextAnnotationSchema}. This {@link StructuredRecord} can then be turned into a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    return getOutputRecordBuilder(input)
        .set(outputFieldName, extractTextAnnotations(annotateImageResponse))
        .build();
  }

  /**
   * Extract a {@link List} of {@link StructuredRecord} containing the text information from a
   * {@link AnnotateImageResponse} input using a {@link Schema} for the mapping.
   *
   * @param annotateImageResponse The {@link AnnotateImageResponse} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private List<StructuredRecord> extractTextAnnotations(AnnotateImageResponse annotateImageResponse) {
    return annotateImageResponse.getTextAnnotationsList().stream()
        .map(this::extractTextAnnotationRecord)
        .collect(Collectors.toList());
  }

  /**
   * Extract a {@link StructuredRecord} containing the text annotation information from a
   * {@link EntityAnnotation} input using a {@link Schema} for the mapping.
   *
   * @param annotation The {@link EntityAnnotation} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractTextAnnotationRecord(EntityAnnotation annotation) {
    Schema textSchema = getTextAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(textSchema);
    if (textSchema.getField(TextAnnotationSchema.LOCALE_FIELD_NAME) != null) {
      builder.set(TextAnnotationSchema.LOCALE_FIELD_NAME, annotation.getLocale());
    }
    if (textSchema.getField(TextAnnotationSchema.DESCRIPTION_FIELD_NAME) != null) {
      builder.set(TextAnnotationSchema.DESCRIPTION_FIELD_NAME, annotation.getDescription());
    }
    Schema.Field positionField = textSchema.getField(TextAnnotationSchema.POSITION_FIELD_NAME);
    if (positionField != null) {
      Schema positionSchema = getComponentSchema(positionField);
      List<StructuredRecord> position = annotation.getBoundingPoly().getVerticesList().stream()
          .map(v -> extractVertex(v, positionSchema))
          .collect(Collectors.toList());
      builder.set(TextAnnotationSchema.POSITION_FIELD_NAME, position);
    }

    return builder.build();
  }

  /**
   * Retrieves Text Annotation's non-nullable component schema. Text Annotation's schema is retrieved instead of using
   * constant schema since users are free to choose to not include some of the fields.
   *
   * @return Text Annotation's non-nullable component schema.
   */
  private Schema getTextAnnotationSchema() {
    Schema.Field textAnnotationsField = schema.getField(outputFieldName);
    return getComponentSchema(textAnnotationsField);
  }
}
