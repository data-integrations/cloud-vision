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
import com.google.cloud.vision.v1.SafeSearchAnnotation;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.SafeSearchAnnotationSchema;


/**
 * Transforms safe search annotation of specified {@link AnnotateImageResponse} to {@link StructuredRecord} according
 * to the specified schema.
 */
public class SafeSearchAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public SafeSearchAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link SafeSearchAnnotationSchema}. This {@link StructuredRecord} can then be turned into
   * a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    SafeSearchAnnotation annotation = annotateImageResponse.getSafeSearchAnnotation();
    return getOutputRecordBuilder(input)
        .set(outputFieldName, extractSafeSearchAnnotation(annotation))
        .build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the safe search information from a
   * {@link SafeSearchAnnotation} input using a {@link Schema} for the mapping.
   *
   * @param annotation The {@link SafeSearchAnnotation} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractSafeSearchAnnotation(SafeSearchAnnotation annotation) {
    Schema safeSearchAnnotationSchema = getSafeSearchAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(safeSearchAnnotationSchema);
    if (safeSearchAnnotationSchema.getField(SafeSearchAnnotationSchema.ADULT_FIELD_NAME) != null) {
      builder.set(SafeSearchAnnotationSchema.ADULT_FIELD_NAME, annotation.getAdult().name());
    }
    if (safeSearchAnnotationSchema.getField(SafeSearchAnnotationSchema.SPOOF_FIELD_NAME) != null) {
      builder.set(SafeSearchAnnotationSchema.SPOOF_FIELD_NAME, annotation.getSpoof().name());
    }
    if (safeSearchAnnotationSchema.getField(SafeSearchAnnotationSchema.MEDICAL_FIELD_NAME) != null) {
      builder.set(SafeSearchAnnotationSchema.MEDICAL_FIELD_NAME, annotation.getMedical().name());
    }
    if (safeSearchAnnotationSchema.getField(SafeSearchAnnotationSchema.VIOLENCE_FIELD_NAME) != null) {
      builder.set(SafeSearchAnnotationSchema.VIOLENCE_FIELD_NAME, annotation.getViolence().name());
    }
    if (safeSearchAnnotationSchema.getField(SafeSearchAnnotationSchema.RACY_FIELD_NAME) != null) {
      builder.set(SafeSearchAnnotationSchema.RACY_FIELD_NAME, annotation.getRacy().name());
    }

    return builder.build();
  }

  /**
   * Retrieves Safe Search Annotation's non-nullable component schema. Schema retrieved instead of using constant schema
   * since users are free to choose to not include some of the fields
   *
   * @return Safe Search Annotation's non-nullable component schema.
   */
  private Schema getSafeSearchAnnotationSchema() {
    Schema safeSearchAnnotationsFieldSchema = schema.getField(outputFieldName).getSchema();
    return safeSearchAnnotationsFieldSchema.isNullable() ? safeSearchAnnotationsFieldSchema.getNonNullable()
        : safeSearchAnnotationsFieldSchema;
  }
}
