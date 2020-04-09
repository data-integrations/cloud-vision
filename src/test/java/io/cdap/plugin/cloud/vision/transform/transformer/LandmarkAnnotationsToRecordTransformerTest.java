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
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.schema.EntityAnnotationWithPositionSchema;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/**
 * {@link LandmarkAnnotationsToRecordTransformer} test.
 */
public class LandmarkAnnotationsToRecordTransformerTest extends LabelAnnotationsToRecordTransformerTest {

  private static final EntityAnnotation LANDMARK_ANNOTATION = EntityAnnotation.newBuilder()
    .setMid("/m/0dx1j")
    .setDescription("Some Label")
    .setLocale("en")
    .setScore(0.87f)
    .setTopicality(0.21f)
    .addLocations(LOCATION)
    .addProperties(PROPERTY_1)
    .addProperties(PROPERTY_2)
    .setBoundingPoly(POSITION)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .addLandmarkAnnotations(LANDMARK_ANNOTATION)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.LANDMARKS.getSchema()));

    LandmarkAnnotationsToRecordTransformer transformer = new LandmarkAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(output);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    assertAnnotationEquals(LANDMARK_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.LANDMARKS.getSchema()));

    LandmarkAnnotationsToRecordTransformer transformer = new LandmarkAnnotationsToRecordTransformer(schema, output);

    EntityAnnotation emptyAnnotation = EntityAnnotation.newBuilder().build();
    AnnotateImageResponse emptyLandmarkAnnotation = AnnotateImageResponse.newBuilder()
      .addLandmarkAnnotations(emptyAnnotation)
      .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, emptyLandmarkAnnotation);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(output);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    assertAnnotationEquals(emptyAnnotation, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformSingleField() {
    String output = "extracted";
    Schema labelAnnotationSingleFieldSchema = Schema.recordOf("single-label-field", Schema.Field.of(
      EntityAnnotationWithPositionSchema.DESCRIPTION_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, Schema.arrayOf(labelAnnotationSingleFieldSchema)));

    LandmarkAnnotationsToRecordTransformer transformer = new LandmarkAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(output);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    // actual record has single-field schema
    Assert.assertEquals(labelAnnotationSingleFieldSchema, actual.getSchema());
    Assert.assertEquals(LANDMARK_ANNOTATION.getDescription(),
      actual.get(EntityAnnotationWithPositionSchema.DESCRIPTION_FIELD_NAME));
  }

  @Override
  protected void assertAnnotationEquals(EntityAnnotation expected, StructuredRecord actual) {
    // Landmark annotations are mapped in the same way as Label annotation except of additional 'position' field
    super.assertAnnotationEquals(expected, actual);
    List<StructuredRecord> pos = actual.get(EntityAnnotationWithPositionSchema.POSITION_FIELD_NAME);
    assertPositionEqual(expected.getBoundingPoly(), pos);
  }
}
