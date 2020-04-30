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
import io.cdap.plugin.cloud.vision.transform.schema.TextAnnotationSchema;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/**
 * {@link TextAnnotationsToRecordTransformer} test.
 */
public class TextAnnotationsToRecordTransformerTest extends BaseAnnotationsToRecordTransformerTest {

  private static final EntityAnnotation TEXT_ANNOTATION = EntityAnnotation.newBuilder()
    .setLocale("en")
    .setDescription("Some Text")
    .setBoundingPoly(POSITION)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .addTextAnnotations(TEXT_ANNOTATION)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String outputFieldName = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(outputFieldName, ImageFeature.TEXT.getSchema()));

    TextAnnotationsToRecordTransformer transformer = new TextAnnotationsToRecordTransformer(schema, outputFieldName);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    assertAnnotationEquals(TEXT_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String outputFieldName = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(outputFieldName, ImageFeature.TEXT.getSchema()));

    TextAnnotationsToRecordTransformer transformer = new TextAnnotationsToRecordTransformer(schema, outputFieldName);

    EntityAnnotation emptyAnnotation = EntityAnnotation.newBuilder().build();
    AnnotateImageResponse emptyTextAnnotation = AnnotateImageResponse.newBuilder()
      .addTextAnnotations(emptyAnnotation)
      .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, emptyTextAnnotation);

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
    Schema textAnnotationSingleFieldSchema = Schema.recordOf(
      "single-text-field",
      Schema.Field.of(TextAnnotationSchema.DESCRIPTION_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(outputFieldName, Schema.arrayOf(textAnnotationSingleFieldSchema)));

    TextAnnotationsToRecordTransformer transformer = new TextAnnotationsToRecordTransformer(schema, outputFieldName);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(outputFieldName);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    // actual record has single-field schema
    Assert.assertEquals(textAnnotationSingleFieldSchema, actual.getSchema());
    Assert.assertEquals(TEXT_ANNOTATION.getDescription(), actual.get(TextAnnotationSchema.DESCRIPTION_FIELD_NAME));
  }

  private void assertAnnotationEquals(EntityAnnotation expected, StructuredRecord actual) {
    Assert.assertEquals(expected.getLocale(), actual.get(TextAnnotationSchema.LOCALE_FIELD_NAME));
    Assert.assertEquals(expected.getDescription(), actual.get(TextAnnotationSchema.DESCRIPTION_FIELD_NAME));

    List<StructuredRecord> position = actual.get(TextAnnotationSchema.POSITION_FIELD_NAME);
    assertPositionEqual(expected.getBoundingPoly(), position);
  }
}
