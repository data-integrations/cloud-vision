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
import com.google.cloud.vision.v1.Property;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.EntityAnnotationWithPositionSchema;

import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms landmark annotations of specified {@link AnnotateImageResponse} to {@link StructuredRecord} according to
 * the specified schema.
 */
public class LandmarkAnnotationsToRecordTransformer extends LabelAnnotationsToRecordTransformer {

  public LandmarkAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  @Override
  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link io.cdap.plugin.cloud.vision.transform.schema.LocalizedObjectAnnotationSchema}.
   * This {@link StructuredRecord} can then be turned into a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    return getOutputRecordBuilder(input)
        .set(outputFieldName, extractLandmarkAnnotations(annotateImageResponse))
        .build();
  }

  /**
   * Extract a {@link List} of {@link StructuredRecord} containing the landmark information from a
   * {@link AnnotateImageResponse} input using a {@link Schema} for the mapping.
   *
   * @param annotateImageResponse The {@link AnnotateImageResponse} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private List<StructuredRecord> extractLandmarkAnnotations(AnnotateImageResponse annotateImageResponse) {
    return annotateImageResponse.getLandmarkAnnotationsList().stream()
        .map(this::extractAnnotation)
        .collect(Collectors.toList());
  }

  /**
   * Extract a {@link StructuredRecord} containing the entity information from a
   * {@link EntityAnnotation} input using a {@link Schema} for the mapping.
   *
   * @param annotation The {@link EntityAnnotation} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  @Override
  protected StructuredRecord extractAnnotation(EntityAnnotation annotation) {
    Schema landmarkSchema = getEntityAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(landmarkSchema);
    // Landmark annotations are mapped in the same way as Label annotation except of additional 'position' field
    StructuredRecord recordWithoutPosition = super.extractAnnotation(annotation);
    for (Schema.Field field : recordWithoutPosition.getSchema().getFields()) {
      builder.set(field.getName(), recordWithoutPosition.get(field.getName()));
    }
    // Extract position
    Schema schema = super.getEntityAnnotationSchema();
    Schema.Field posField = schema.getField(EntityAnnotationWithPositionSchema.POSITION_FIELD_NAME);
    if (posField != null) {
      Schema positionSchema = getComponentSchema(posField);
      List<StructuredRecord> position = annotation.getBoundingPoly().getVerticesList().stream()
          .map(v -> extractVertex(v, positionSchema))
          .collect(Collectors.toList());
      builder.set(EntityAnnotationWithPositionSchema.POSITION_FIELD_NAME, position);
    }

    return builder.build();
  }
}
