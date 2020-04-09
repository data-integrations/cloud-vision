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
import com.google.cloud.vision.v1.CropHint;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.schema.CropHintAnnotationSchema;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/**
 * {@link CropHintsAnnotationsToRecordTransformer} test.
 */
public class CropHintsAnnotationsToRecordTransformerTest extends BaseAnnotationsToRecordTransformerTest {

  private static final CropHint CROP_HINT_1 = CropHint.newBuilder()
    .setConfidence(0.18f)
    .setImportanceFraction(0.18f)
    .setBoundingPoly(POSITION)
    .build();
  private static final CropHint CROP_HINT_2 = CropHint.newBuilder()
    .setConfidence(0.98f)
    .setImportanceFraction(0.98f)
    .setBoundingPoly(POSITION)
    .build();

  private static final CropHintsAnnotation CROP_HINTS_ANNOTATION = CropHintsAnnotation.newBuilder()
    .addCropHints(CROP_HINT_1)
    .addCropHints(CROP_HINT_2)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .setCropHintsAnnotation(CROP_HINTS_ANNOTATION)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.CROP_HINTS.getSchema()));

    CropHintsAnnotationsToRecordTransformer transformer = new CropHintsAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actual = transformed.get(output);
    assertAnnotationEquals(CROP_HINTS_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.CROP_HINTS.getSchema()));

    CropHintsAnnotationsToRecordTransformer transformer = new CropHintsAnnotationsToRecordTransformer(schema, output);

    CropHintsAnnotation emptyAnnotation = CropHintsAnnotation.newBuilder().build();
    AnnotateImageResponse emptyTextAnnotation = AnnotateImageResponse.newBuilder()
      .setCropHintsAnnotation(emptyAnnotation)
      .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, emptyTextAnnotation);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actual = transformed.get(output);
    assertAnnotationEquals(emptyAnnotation, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformSingleField() {
    String output = "extracted";
    Schema textAnnotationSingleFieldSchema = Schema.recordOf(
      "single-crop-hint-field",
      Schema.Field.of(CropHintAnnotationSchema.CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)));
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, Schema.arrayOf(textAnnotationSingleFieldSchema)));

    CropHintsAnnotationsToRecordTransformer transformer = new CropHintsAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);
    Assert.assertNotNull(transformed);
    List<StructuredRecord> actual = transformed.get(output);

    Assert.assertNotNull(actual);
    Assert.assertEquals(CROP_HINTS_ANNOTATION.getCropHintsCount(), actual.size());
    for (int i = 0; i < CROP_HINTS_ANNOTATION.getCropHintsCount(); i++) {
      CropHint expectedCropHint = CROP_HINTS_ANNOTATION.getCropHints(i);
      StructuredRecord actualCropHint = actual.get(i);
      Assert.assertEquals(expectedCropHint.getConfidence(),
        actualCropHint.<Float>get(CropHintAnnotationSchema.CONFIDENCE_FIELD_NAME),
        DELTA);
    }
  }

  private void assertAnnotationEquals(CropHintsAnnotation expected, List<StructuredRecord> actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getCropHintsCount(), actual.size());
    for (int i = 0; i < expected.getCropHintsCount(); i++) {
      CropHint expectedCropHint = expected.getCropHints(i);
      StructuredRecord actualCropHint = actual.get(i);
      assertCropHintEquals(expectedCropHint, actualCropHint);
    }
  }

  private void assertCropHintEquals(CropHint expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getConfidence(),
      actual.<Float>get(CropHintAnnotationSchema.CONFIDENCE_FIELD_NAME),
      DELTA);
    Assert.assertEquals(expected.getImportanceFraction(),
      actual.<Float>get(CropHintAnnotationSchema.IMPORTANCE_FRACTION_FIELD_NAME),
      DELTA);
  }
}
