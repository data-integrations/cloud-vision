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
import com.google.cloud.vision.v1.Likelihood;
import com.google.cloud.vision.v1.Position;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.schema.FaceAnnotationSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * {@link FaceAnnotationsToRecordTransformer} test.
 */
public class FaceAnnotationsToRecordTransformerTest extends BaseAnnotationsToRecordTransformerTest {

  private static final FaceAnnotation FACE_ANNOTATION = FaceAnnotation.newBuilder()
          .setAngerLikelihood(Likelihood.UNLIKELY)
          .setBlurredLikelihood(Likelihood.LIKELY)
          .setHeadwearLikelihood(Likelihood.UNLIKELY)
          .setJoyLikelihood(Likelihood.UNLIKELY)
          .setSorrowLikelihood(Likelihood.LIKELY)
          .setSurpriseLikelihood(Likelihood.UNLIKELY)
          .setUnderExposedLikelihood(Likelihood.POSSIBLE)
          .setPanAngle(0.1f)
          .setRollAngle(0.2f)
          .setTiltAngle(0.3f)
          .setDetectionConfidence(99.9f)
          .setLandmarkingConfidence(09.9f)
          .setBoundingPoly(POSITION)
          .setFdBoundingPoly(POSITION)
          .addLandmarks(
                  FaceAnnotation.Landmark.newBuilder()
                          .setType(FaceAnnotation.Landmark.Type.CHIN_GNATHION)
                          .setPosition(Position.newBuilder().setX(10.1f).setY(10.1f).setZ(10.1f))
          )
          .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
          .addFaceAnnotations(FACE_ANNOTATION)
          .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String outputFieldName = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
            Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
            Schema.Field.of(outputFieldName, ImageFeature.FACE.getSchema()));

    FaceAnnotationsToRecordTransformer transformer = new FaceAnnotationsToRecordTransformer(schema, outputFieldName);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    assertAnnotationEquals(FACE_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String outputFieldName = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
            Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
            Schema.Field.of(outputFieldName, ImageFeature.FACE.getSchema()));

    FaceAnnotationsToRecordTransformer transformer = new FaceAnnotationsToRecordTransformer(schema, outputFieldName);

    FaceAnnotation emptyAnnotation = FaceAnnotation.newBuilder().build();
    AnnotateImageResponse emptyFaceAnnotation = AnnotateImageResponse.newBuilder()
            .addFaceAnnotations(emptyAnnotation)
            .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, emptyFaceAnnotation);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    assertAnnotationEquals(emptyAnnotation, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformSingleField() {
    String outputFieldName = "extracted";
    Schema faceAnnotationSingleFieldSchema = Schema.recordOf(
            "single-face-field",
            Schema.Field.of(FaceAnnotationSchema.ANGER_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("transformed-record-schema",
            Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
            Schema.Field.of(outputFieldName, Schema.arrayOf(faceAnnotationSingleFieldSchema)));

    FaceAnnotationsToRecordTransformer transformer = new FaceAnnotationsToRecordTransformer(schema, outputFieldName);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    // actual record has single-field schema
    Assert.assertEquals(faceAnnotationSingleFieldSchema, actual.getSchema());
    Assert.assertEquals(FACE_ANNOTATION.getAngerLikelihood().name(),
            actual.get(FaceAnnotationSchema.ANGER_FIELD_NAME));
  }

  private void assertAnnotationEquals(FaceAnnotation expected, StructuredRecord actual) {
    Assert.assertEquals(expected.getDetectionConfidence(),
            actual.<Float>get(FaceAnnotationSchema.DETECTION_CONFIDENCE_FIELD_NAME),
            DELTA);
    Assert.assertEquals(expected.getLandmarkingConfidence(),
            actual.<Float>get(FaceAnnotationSchema.LANDMARKING_CONFIDENCE_FIELD_NAME),
            DELTA);
    Assert.assertEquals(expected.getRollAngle(),
            actual.<Float>get(FaceAnnotationSchema.ROLL_ANGLE_FIELD_NAME),
            DELTA);
    Assert.assertEquals(expected.getPanAngle(),
            actual.<Float>get(FaceAnnotationSchema.PAN_ANGLE_FIELD_NAME),
            DELTA);
    Assert.assertEquals(expected.getTiltAngle(),
            actual.<Float>get(FaceAnnotationSchema.TILT_ANGLE_FIELD_NAME),
            DELTA);
    Assert.assertEquals(expected.getAngerLikelihood().name(), actual.get(FaceAnnotationSchema.ANGER_FIELD_NAME));
    Assert.assertEquals(expected.getBlurredLikelihood().name(), actual.get(FaceAnnotationSchema.BLURRED_FIELD_NAME));
    Assert.assertEquals(expected.getHeadwearLikelihood().name(), actual.get(FaceAnnotationSchema.HEADWEAR_FIELD_NAME));
    Assert.assertEquals(expected.getSorrowLikelihood().name(), actual.get(FaceAnnotationSchema.SORROW_FIELD_NAME));
    Assert.assertEquals(expected.getJoyLikelihood().name(), actual.get(FaceAnnotationSchema.JOY_FIELD_NAME));
    Assert.assertEquals(expected.getSurpriseLikelihood().name(), actual.get(FaceAnnotationSchema.SURPRISE_FIELD_NAME));
    Assert.assertEquals(expected.getUnderExposedLikelihood().name(),
            actual.get(FaceAnnotationSchema.UNDER_EXPOSED_FIELD_NAME));

    List<StructuredRecord> position = actual.get(FaceAnnotationSchema.POSITION_FIELD_NAME);
    assertPositionEqual(expected.getBoundingPoly(), position);
    List<StructuredRecord> fdPosition = actual.get(FaceAnnotationSchema.FD_POSITION_FIELD_NAME);
    assertPositionEqual(expected.getFdBoundingPoly(), fdPosition);
    List<StructuredRecord> landmarks = actual.get(FaceAnnotationSchema.LANDMARKS_FIELD_NAME);
    assertLandmarksEqual(expected.getLandmarksList(), landmarks);
  }

  private void assertLandmarksEqual(List<FaceAnnotation.Landmark> expected, List<StructuredRecord> actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      FaceAnnotation.Landmark landmark = expected.get(i);
      StructuredRecord actualLandmark = actual.get(i);
      Assert.assertNotNull(actualLandmark);

      Assert.assertEquals(landmark.getType(), actualLandmark.get(FaceAnnotationSchema.FaceLandmark.TYPE_FIELD_NAME));
      Assert.assertEquals(landmark.getPosition().getX(),
              actualLandmark.<Float>get(FaceAnnotationSchema.FaceLandmark.X_FIELD_NAME),
              DELTA);
      Assert.assertEquals(landmark.getPosition().getY(),
              actualLandmark.<Float>get(FaceAnnotationSchema.FaceLandmark.Y_FIELD_NAME),
              DELTA);
      Assert.assertEquals(landmark.getPosition().getZ(),
              actualLandmark.<Float>get(FaceAnnotationSchema.FaceLandmark.Z_FIELD_NAME),
              DELTA);
    }
  }
}
