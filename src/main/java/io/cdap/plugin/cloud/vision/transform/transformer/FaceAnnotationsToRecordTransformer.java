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
import com.google.cloud.vision.v1.FaceAnnotation;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.FaceAnnotationSchema;

import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms face annotations of specified {@link AnnotateImageResponse} to
 * {@link StructuredRecord} according to the specified schema.
 */
public class FaceAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public FaceAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    StructuredRecord.Builder builder = getOutputRecordBuilder(input);
    List<StructuredRecord> extracted = extractFaceAnnotations(annotateImageResponse);
    return builder.set(outputFieldName, extracted).build();
  }

  public StructuredRecord.Builder getBuilder(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    StructuredRecord.Builder builder = getOutputRecordBuilder(input);
    return builder;
  }

  private List<StructuredRecord> extractFaceAnnotations(AnnotateImageResponse annotateImageResponse) {
    return annotateImageResponse.getFaceAnnotationsList().stream()
            .map(this::extractFaceAnnotationRecord)
            .collect(Collectors.toList());
  }

  private StructuredRecord extractFaceAnnotationRecord(FaceAnnotation annotation) {
    Schema faceSchema = getFaceAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(faceSchema);
    if (faceSchema.getField(FaceAnnotationSchema.ROLL_ANGLE_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.ROLL_ANGLE_FIELD_NAME, annotation.getRollAngle());
    }
    if (faceSchema.getField(FaceAnnotationSchema.PAN_ANGLE_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.PAN_ANGLE_FIELD_NAME, annotation.getPanAngle());
    }
    if (faceSchema.getField(FaceAnnotationSchema.TILT_ANGLE_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.TILT_ANGLE_FIELD_NAME, annotation.getTiltAngle());
    }
    if (faceSchema.getField(FaceAnnotationSchema.DETECTION_CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.DETECTION_CONFIDENCE_FIELD_NAME,
              annotation.getDetectionConfidence());
    }
    if (faceSchema.getField(FaceAnnotationSchema.LANDMARKING_CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.LANDMARKING_CONFIDENCE_FIELD_NAME,
              annotation.getLandmarkingConfidence());
    }
    if (faceSchema.getField(FaceAnnotationSchema.ANGER_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.ANGER_FIELD_NAME, annotation.getAngerLikelihood().name());
    }
    if (faceSchema.getField(FaceAnnotationSchema.JOY_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.JOY_FIELD_NAME, annotation.getJoyLikelihood().name());
    }
    if (faceSchema.getField(FaceAnnotationSchema.BLURRED_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.BLURRED_FIELD_NAME, annotation.getBlurredLikelihood().name());
    }
    if (faceSchema.getField(FaceAnnotationSchema.SORROW_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.SORROW_FIELD_NAME, annotation.getSorrowLikelihood().name());
    }
    if (faceSchema.getField(FaceAnnotationSchema.UNDER_EXPOSED_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.UNDER_EXPOSED_FIELD_NAME, annotation.getUnderExposedLikelihood().name());
    }
    if (faceSchema.getField(FaceAnnotationSchema.HEADWEAR_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.HEADWEAR_FIELD_NAME, annotation.getHeadwearLikelihood().name());
    }
    if (faceSchema.getField(FaceAnnotationSchema.SURPRISE_FIELD_NAME) != null) {
      String surprise = annotation.getSurpriseLikelihood().name();
      builder.set(FaceAnnotationSchema.SURPRISE_FIELD_NAME, surprise);
    }
    Schema.Field positionField = faceSchema.getField(FaceAnnotationSchema.BOUNDING_POLY_NAME);
    if (positionField != null) {
      Schema positionSchema = getComponentSchema(positionField);
      List<StructuredRecord> position = annotation.getBoundingPoly().getVerticesList().stream()
              .map(v -> extractVertex(v, positionSchema))
              .collect(Collectors.toList());
      builder.set(FaceAnnotationSchema.BOUNDING_POLY_NAME, position);
    }
    Schema.Field fdPositionField = faceSchema.getField(FaceAnnotationSchema.FD_BOUNDING_POLY_NAME);
    if (fdPositionField != null) {
      Schema positionSchema = getComponentSchema(fdPositionField);
      List<StructuredRecord> position = annotation.getFdBoundingPoly().getVerticesList().stream()
              .map(v -> extractVertex(v, positionSchema))
              .collect(Collectors.toList());
      builder.set(FaceAnnotationSchema.FD_BOUNDING_POLY_NAME, position);
    }
    Schema.Field landmarksField = faceSchema.getField(FaceAnnotationSchema.LANDMARKS_FIELD_NAME);
    if (landmarksField != null) {
      Schema landmarkSchema = getComponentSchema(landmarksField);
      List<StructuredRecord> position = annotation.getLandmarksList().stream()
              .map(v -> extractLandmark(v, landmarkSchema))
              .collect(Collectors.toList());
      builder.set(FaceAnnotationSchema.LANDMARKS_FIELD_NAME, position);
    }

    return builder.build();
  }

  private StructuredRecord extractLandmark(FaceAnnotation.Landmark landmark, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(FaceAnnotationSchema.FaceLandmark.TYPE_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.FaceLandmark.TYPE_FIELD_NAME, landmark.getType());
    }
    if (schema.getField(FaceAnnotationSchema.FaceLandmark.X_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.FaceLandmark.X_FIELD_NAME, landmark.getPosition().getX());
    }
    if (schema.getField(FaceAnnotationSchema.FaceLandmark.Y_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.FaceLandmark.Y_FIELD_NAME, landmark.getPosition().getY());
    }
    if (schema.getField(FaceAnnotationSchema.FaceLandmark.Z_FIELD_NAME) != null) {
      builder.set(FaceAnnotationSchema.FaceLandmark.Z_FIELD_NAME, landmark.getPosition().getZ());
    }

    return builder.build();
  }

  /**
   * Retrieves Face Annotation's non-nullable component schema. Face Annotation's schema is retrieved instead of using
   * constant schema since users are free to choose to not include some of the fields.
   *
   * @return Face Annotation's non-nullable component schema.
   */
  private Schema getFaceAnnotationSchema() {
    Schema.Field faceAnnotationsField = schema.getField(outputFieldName);
    return getComponentSchema(faceAnnotationsField);
  }
}
