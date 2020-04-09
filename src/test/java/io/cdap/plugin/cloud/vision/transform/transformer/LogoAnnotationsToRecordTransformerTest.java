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
 * {@link LogoAnnotationsToRecordTransformer} test.
 */
public class LogoAnnotationsToRecordTransformerTest extends LandmarkAnnotationsToRecordTransformerTest {

  private static final EntityAnnotation LOGO_ANNOTATION = EntityAnnotation.newBuilder()
    .setMid("/m/0dx1j")
    .setDescription("Some Logo")
    .setLocale("en")
    .setScore(0.87f)
    .setTopicality(0.21f)
    .addLocations(LOCATION)
    .addProperties(PROPERTY_1)
    .addProperties(PROPERTY_2)
    .setBoundingPoly(POSITION)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .addLogoAnnotations(LOGO_ANNOTATION)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.LOGOS.getSchema()));

    LogoAnnotationsToRecordTransformer transformer = new LogoAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(output);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    assertAnnotationEquals(LOGO_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.LOGOS.getSchema()));

    LogoAnnotationsToRecordTransformer transformer = new LogoAnnotationsToRecordTransformer(schema, output);

    EntityAnnotation emptyAnnotation = EntityAnnotation.newBuilder().build();
    AnnotateImageResponse emptyLogoAnnotation = AnnotateImageResponse.newBuilder()
      .addLogoAnnotations(emptyAnnotation)
      .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, emptyLogoAnnotation);

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

    LogoAnnotationsToRecordTransformer transformer = new LogoAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actualExtracted = transformed.get(output);
    Assert.assertNotNull(actualExtracted);
    Assert.assertEquals(1, actualExtracted.size());
    StructuredRecord actual = actualExtracted.get(SINGLE_FEATURE_INDEX);
    // actual record has single-field schema
    Assert.assertEquals(labelAnnotationSingleFieldSchema, actual.getSchema());
    Assert.assertEquals(LOGO_ANNOTATION.getDescription(),
      actual.get(EntityAnnotationWithPositionSchema.DESCRIPTION_FIELD_NAME));
  }
}
