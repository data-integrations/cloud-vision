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
import com.google.cloud.vision.v1.LocationInfo;
import com.google.cloud.vision.v1.Property;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.EntityAnnotationSchema;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms label annotations of specified {@link AnnotateImageResponse} to {@link StructuredRecord} according to the
 * specified schema.
 */
public class LabelAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public LabelAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
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
            .set(outputFieldName, extractLabelAnnotations(annotateImageResponse))
            .build();
  }

  /**
   * Extract a {@link List} of {@link StructuredRecord} containing the label information from a
   * {@link AnnotateImageResponse} input using a {@link Schema} for the mapping.
   *
   * @param annotateImageResponse The {@link AnnotateImageResponse} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private List<StructuredRecord> extractLabelAnnotations(AnnotateImageResponse annotateImageResponse) {
    return annotateImageResponse.getLabelAnnotationsList().stream()
            .map(this::extractAnnotation)
            .collect(Collectors.toList());
  }

  /**
   * Extract a {@link List} of {@link StructuredRecord} containing the entity annotation information from a
   * {@link AnnotateImageResponse} input using a {@link Schema} for the mapping.
   *
   * @param annotation The {@link EntityAnnotation} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  protected StructuredRecord extractAnnotation(EntityAnnotation annotation) {
    Schema labelSchema = getEntityAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(labelSchema);
    if (labelSchema.getField(EntityAnnotationSchema.MID_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.MID_FIELD_NAME, annotation.getMid());
    }
    if (labelSchema.getField(EntityAnnotationSchema.LOCALE_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.LOCALE_FIELD_NAME, annotation.getLocale());
    }
    if (labelSchema.getField(EntityAnnotationSchema.DESCRIPTION_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.DESCRIPTION_FIELD_NAME, annotation.getDescription());
    }
    if (labelSchema.getField(EntityAnnotationSchema.SCORE_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.SCORE_FIELD_NAME, annotation.getScore());
    }
    if (labelSchema.getField(EntityAnnotationSchema.TOPICALITY_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.TOPICALITY_FIELD_NAME, annotation.getTopicality());
    }
    Schema.Field locField = labelSchema.getField(EntityAnnotationSchema.LOCATIONS_FIELD_NAME);
    if (locField != null) {
      Schema locationSchema = getComponentSchema(locField);
      List<StructuredRecord> location = annotation.getLocationsList().stream()
              .map(v -> extractLocation(v, locationSchema))
              .collect(Collectors.toList());
      builder.set(EntityAnnotationSchema.LOCATIONS_FIELD_NAME, location);
    }
    Schema.Field propField = labelSchema.getField(EntityAnnotationSchema.PROPERTIES_FIELD_NAME);
    if (propField != null) {
      Schema propertySchema = getComponentSchema(propField);
      List<StructuredRecord> location = annotation.getPropertiesList().stream()
              .map(v -> extractProperty(v, propertySchema))
              .collect(Collectors.toList());
      builder.set(EntityAnnotationSchema.PROPERTIES_FIELD_NAME, location);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the location information from a
   * {@link LocationInfo} input using a {@link Schema} for the mapping.
   *
   * @param locationInfo The {@link LocationInfo} object containing the data.
   * @param schema The {@link Schema} to use for the mapping.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  protected StructuredRecord extractLocation(LocationInfo locationInfo, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(EntityAnnotationSchema.LocationInfo.LATITUDE_FIELD_NAME) != null) {
      double latitude = locationInfo.getLatLng().getLatitude();
      builder.set(EntityAnnotationSchema.LocationInfo.LATITUDE_FIELD_NAME, latitude);
    }
    if (schema.getField(EntityAnnotationSchema.LocationInfo.LONGITUDE_FIELD_NAME) != null) {
      double longitude = locationInfo.getLatLng().getLongitude();
      builder.set(EntityAnnotationSchema.LocationInfo.LONGITUDE_FIELD_NAME, longitude);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the property information from a
   * {@link Property} input using a {@link Schema} for the mapping.
   *
   * @param property The {@link Property} object containing the data.
   * @param schema   The {@link Schema} to use for the mapping.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  protected StructuredRecord extractProperty(Property property, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(EntityAnnotationSchema.Property.NAME_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.Property.NAME_FIELD_NAME, property.getName());
    }
    if (schema.getField(EntityAnnotationSchema.Property.VALUE_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.Property.VALUE_FIELD_NAME, property.getValue());
    }
    if (schema.getField(EntityAnnotationSchema.Property.UINT_64_VALUE_FIELD_NAME) != null) {
      builder.set(EntityAnnotationSchema.Property.UINT_64_VALUE_FIELD_NAME, property.getUint64Value());
    }

    return builder.build();
  }

  /**
   * Retrieves Entity Annotation's non-nullable component schema. Entity Annotation's schema is retrieved instead of
   * using constant schema since users are free to choose to not include some of the fields.
   *
   * @return Entity Annotation's non-nullable component schema.
   */
  protected Schema getEntityAnnotationSchema() {
    Schema.Field labelAnnotationsField = schema.getField(outputFieldName);
    return getComponentSchema(labelAnnotationsField);
  }
}
