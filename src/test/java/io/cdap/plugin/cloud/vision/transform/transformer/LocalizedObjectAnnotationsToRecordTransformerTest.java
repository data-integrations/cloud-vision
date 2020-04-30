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
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.schema.LocalizedObjectAnnotationSchema;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/**
 * {@link LocalizedObjectAnnotationsToRecordTransformer} test.
 */
public class LocalizedObjectAnnotationsToRecordTransformerTest extends BaseAnnotationsToRecordTransformerTest {

  private static final LocalizedObjectAnnotation LOCALIZED_OBJECT_ANNOTATION = LocalizedObjectAnnotation.newBuilder()
    .setMid("/m/01bqk0")
    .setName("Bicycle wheel")
    .setLanguageCode("en")
    .setScore(0.89648587f)
    .setBoundingPoly(POSITION)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .addLocalizedObjectAnnotations(LOCALIZED_OBJECT_ANNOTATION)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String outputFieldName = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(outputFieldName, ImageFeature.OBJECT_LOCALIZATION.getSchema()));

    LocalizedObjectAnnotationsToRecordTransformer transformer =
      new LocalizedObjectAnnotationsToRecordTransformer(schema, outputFieldName);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    assertAnnotationEquals(LOCALIZED_OBJECT_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String outputFieldName = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(outputFieldName, ImageFeature.OBJECT_LOCALIZATION.getSchema()));

    LocalizedObjectAnnotationsToRecordTransformer transformer =
      new LocalizedObjectAnnotationsToRecordTransformer(schema, outputFieldName);

    LocalizedObjectAnnotation emptyAnnotation = LocalizedObjectAnnotation.newBuilder().build();
    AnnotateImageResponse response = AnnotateImageResponse.newBuilder()
      .addLocalizedObjectAnnotations(emptyAnnotation)
      .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, response);

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
    Schema singleFieldSchema = Schema.recordOf("single-field", Schema.Field.of(
      LocalizedObjectAnnotationSchema.NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(outputFieldName, Schema.arrayOf(singleFieldSchema)));

    LocalizedObjectAnnotationsToRecordTransformer transformer =
      new LocalizedObjectAnnotationsToRecordTransformer(schema, outputFieldName);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    // actual record has single-field schema
    Assert.assertEquals(singleFieldSchema, actual.getSchema());
    Assert.assertEquals(LOCALIZED_OBJECT_ANNOTATION.getName(),
      actual.get(LocalizedObjectAnnotationSchema.NAME_FIELD_NAME));
  }

  private void assertAnnotationEquals(LocalizedObjectAnnotation expected, StructuredRecord actual) {
    Assert.assertEquals(expected.getMid(),
      actual.get(LocalizedObjectAnnotationSchema.MID_FIELD_NAME));
    Assert.assertEquals(expected.getName(),
      actual.get(LocalizedObjectAnnotationSchema.NAME_FIELD_NAME));

    Assert.assertEquals(expected.getScore(),
      actual.<Float>get(LocalizedObjectAnnotationSchema.SCORE_FIELD_NAME),
      DELTA);

    List<StructuredRecord> position = actual.get(LocalizedObjectAnnotationSchema.POSITION_FIELD_NAME);
    assertPositionEqual(expected.getBoundingPoly(), position);
  }
}
