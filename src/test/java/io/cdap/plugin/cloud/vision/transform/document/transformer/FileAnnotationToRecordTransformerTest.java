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

package io.cdap.plugin.cloud.vision.transform.document.transformer;

import com.google.cloud.vision.v1.AnnotateFileResponse;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.BoundingPoly;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.Likelihood;
import com.google.cloud.vision.v1.Position;
import com.google.cloud.vision.v1.Vertex;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfigTest;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.document.DocumentExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.document.DocumentExtractorTransformConfigBuilder;
import io.cdap.plugin.cloud.vision.transform.schema.EntityAnnotationWithPositionSchema;
import io.cdap.plugin.cloud.vision.transform.schema.FaceAnnotationSchema;
import io.cdap.plugin.cloud.vision.transform.schema.VertexSchema;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link FileAnnotationToRecordTransformer} test.
 */
public class FileAnnotationToRecordTransformerTest extends ExtractorTransformConfigTest {

  protected static final int SINGLE_FEATURE_INDEX = 0;
  protected static final double DELTA = 0.0001;

  protected static final BoundingPoly POSITION = BoundingPoly.newBuilder()
          .addAllVertices(Arrays.asList(
                  Vertex.newBuilder().setX(0).setY(0).build(),
                  Vertex.newBuilder().setX(100).setY(0).build(),
                  Vertex.newBuilder().setX(100).setY(100).build(),
                  Vertex.newBuilder().setX(0).setY(100).build()
          ))
          .build();

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

  private static final AnnotateImageResponse IMAGE_RESPONSE = AnnotateImageResponse.newBuilder()
          .addFaceAnnotations(FACE_ANNOTATION)
          .build();

  private static final AnnotateFileResponse RESPONSE = AnnotateFileResponse.newBuilder()
          .addResponses(IMAGE_RESPONSE)
          .build();

  protected static final Schema INPUT_RECORD_SCHEMA = Schema.recordOf(
          "input-record-schema",
          Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  protected static final StructuredRecord INPUT_RECORD = StructuredRecord
          .builder(INPUT_RECORD_SCHEMA)
          .set("offset", 0)
          .set("body", "gs://dummy/image.png")
          .build();

  @Override
  protected DocumentExtractorTransformConfigBuilder getValidConfigBuilder() {
    return DocumentExtractorTransformConfigBuilder.builder()
            .setPathField("body")
            .setOutputField("feature")
            .setFeatures(ImageFeature.FACE.getDisplayName())
            .setMimeType("application/pdf")
            .setSchema(VALID_SCHEMA.toString());
  }

  private Schema pagesSchema(Schema imageFeatureSchema) {
    return Schema.arrayOf(
            Schema.recordOf("page-record",
                    Schema.Field.of("page", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("feature", imageFeatureSchema)));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    List<Schema.Field> fields = new ArrayList<>();
    String outputFieldName = "feature";

    // Add the input fields
    fields.add(Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));
    fields.add(Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

    // Add the fields of the image feature schema
    Schema pagesSchema = pagesSchema(ImageFeature.FACE.getSchema());
    fields.add(Schema.Field.of(outputFieldName, pagesSchema));
    // Build a schema combining all
    Schema schema = Schema.recordOf("page-record", fields);

    DocumentExtractorTransformConfig config = getValidConfigBuilder()
            .setPages("1")
            .setProject("cdap-vision-test01")
            .setServiceFilePath("/tmp/dummy.json")
            .build();
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());

    FileAnnotationToRecordTransformer transformer = new FileAnnotationToRecordTransformer(config, schema);


    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);
    Assert.assertNotNull(transformed);

    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());

    StructuredRecord actual = actualExtracted.get(0);
    assertAnnotationEquals(FACE_ANNOTATION, actual);
  }

  protected void assertAnnotationEquals(FaceAnnotation expected, StructuredRecord actual) {
    Assert.assertEquals(expected.getAngerLikelihood(), actual.get(FaceAnnotationSchema.ANGER_FIELD_NAME));
    Assert.assertEquals(expected.getBlurredLikelihood(), actual.get(FaceAnnotationSchema.BLURRED_FIELD_NAME));
    Assert.assertEquals(expected.getHeadwearLikelihood(), actual.get(FaceAnnotationSchema.HEADWEAR_FIELD_NAME));
    Assert.assertEquals(expected.getJoyLikelihood(), actual.get(FaceAnnotationSchema.JOY_FIELD_NAME));
    Assert.assertEquals(expected.getSorrowLikelihood(), actual.get(FaceAnnotationSchema.SORROW_FIELD_NAME));
    Assert.assertEquals(expected.getSurpriseLikelihood(), actual.get(FaceAnnotationSchema.SURPRISE_FIELD_NAME));
    Assert.assertEquals(expected.getUnderExposedLikelihood(),
            actual.get(FaceAnnotationSchema.UNDER_EXPOSED_FIELD_NAME));
    Assert.assertEquals(expected.getPanAngle(),
            actual.get(FaceAnnotationSchema.PAN_ANGLE_FIELD_NAME),
            DELTA);

    Assert.assertEquals(expected.getRollAngle(),
            actual.get(FaceAnnotationSchema.ROLL_ANGLE_FIELD_NAME),
            DELTA);

    Assert.assertEquals(expected.getTiltAngle(),
            actual.get(FaceAnnotationSchema.TILT_ANGLE_FIELD_NAME),
            DELTA);

    Assert.assertEquals(expected.getDetectionConfidence(),
            actual.get(FaceAnnotationSchema.DETECTION_CONFIDENCE_FIELD_NAME),
            DELTA);

    Assert.assertEquals(expected.getLandmarkingConfidence(),
            actual.get(FaceAnnotationSchema.LANDMARKING_CONFIDENCE_FIELD_NAME),
            DELTA);
    List<StructuredRecord> position = actual.get(FaceAnnotationSchema.BOUNDING_POLY_NAME);
    assertPositionEqual(expected.getBoundingPoly(), position);
    List<StructuredRecord> fdPosition = actual.get(FaceAnnotationSchema.FD_BOUNDING_POLY_NAME);
    assertPositionEqual(expected.getFdBoundingPoly(), fdPosition);
    List<StructuredRecord> landmarks = actual.get(FaceAnnotationSchema.LANDMARKS_FIELD_NAME);
    assertLandmarksEqual(expected.getLandmarksList(), landmarks);
    List<StructuredRecord> pos = actual.get(EntityAnnotationWithPositionSchema.POSITION_FIELD_NAME);
    assertPositionEqual(expected.getBoundingPoly(), pos);
  }

  protected void assertPositionEqual(BoundingPoly expected, List<StructuredRecord> actual) {
    Assert.assertNotNull(actual);
    for (int i = 0; i < expected.getVerticesList().size(); i++) {
      Vertex expectedVertex = expected.getVertices(i);
      StructuredRecord actualVertex = actual.get(i);
      Assert.assertNotNull(actualVertex);

      Assert.assertEquals(expectedVertex.getX(), (int) actualVertex.get(VertexSchema.X_FIELD_NAME));
      Assert.assertEquals(expectedVertex.getY(), (int) actualVertex.get(VertexSchema.Y_FIELD_NAME));
    }
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
