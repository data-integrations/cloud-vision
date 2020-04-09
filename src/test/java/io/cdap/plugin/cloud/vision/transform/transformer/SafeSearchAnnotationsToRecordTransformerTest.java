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
import com.google.cloud.vision.v1.Likelihood;
import com.google.cloud.vision.v1.SafeSearchAnnotation;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.schema.SafeSearchAnnotationSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link SafeSearchAnnotationsToRecordTransformer} test.
 */
public class SafeSearchAnnotationsToRecordTransformerTest extends BaseAnnotationsToRecordTransformerTest {

  private static final SafeSearchAnnotation SAFE_SEARCH_ANNOTATION = SafeSearchAnnotation.newBuilder()
    .setAdult(Likelihood.UNLIKELY)
    .setSpoof(Likelihood.POSSIBLE)
    .setMedical(Likelihood.UNLIKELY)
    .setViolence(Likelihood.POSSIBLE)
    .setRacy(Likelihood.UNLIKELY)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .setSafeSearchAnnotation(SAFE_SEARCH_ANNOTATION)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.EXPLICIT_CONTENT.getSchema()));

    SafeSearchAnnotationsToRecordTransformer transformer = new SafeSearchAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);
    Assert.assertNotNull(transformed);
    StructuredRecord actual = transformed.get(output);
    assertAnnotationEquals(SAFE_SEARCH_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.EXPLICIT_CONTENT.getSchema()));

    SafeSearchAnnotationsToRecordTransformer transformer = new SafeSearchAnnotationsToRecordTransformer(schema, output);

    SafeSearchAnnotation emptyAnnotation = SafeSearchAnnotation.newBuilder().build();
    AnnotateImageResponse response = AnnotateImageResponse.newBuilder()
      .setSafeSearchAnnotation(emptyAnnotation)
      .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, response);
    Assert.assertNotNull(transformed);
    StructuredRecord actual = transformed.get(output);
    assertAnnotationEquals(emptyAnnotation, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformSingleField() {
    String output = "extracted";
    Schema singleFieldSchema = Schema.recordOf("single-field", Schema.Field.of(
      SafeSearchAnnotationSchema.VIOLENCE_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, singleFieldSchema));

    SafeSearchAnnotationsToRecordTransformer transformer = new SafeSearchAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    StructuredRecord actual = transformed.get(output);
    // actual record has single-field schema
    Assert.assertEquals(singleFieldSchema, actual.getSchema());
    Assert.assertEquals(SAFE_SEARCH_ANNOTATION.getViolence().name(),
      actual.get(SafeSearchAnnotationSchema.VIOLENCE_FIELD_NAME));
  }

  private void assertAnnotationEquals(SafeSearchAnnotation expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    Likelihood adult = expected.getAdult();
    Assert.assertEquals(adult.name(), actual.get(SafeSearchAnnotationSchema.ADULT_FIELD_NAME));

    Likelihood violence = expected.getViolence();
    Assert.assertEquals(violence.name(), actual.get(SafeSearchAnnotationSchema.VIOLENCE_FIELD_NAME));

    Likelihood spoof = expected.getSpoof();
    Assert.assertEquals(spoof.name(), actual.get(SafeSearchAnnotationSchema.SPOOF_FIELD_NAME));

    Likelihood medical = expected.getMedical();
    Assert.assertEquals(medical.name(), actual.get(SafeSearchAnnotationSchema.MEDICAL_FIELD_NAME));

    Likelihood racy = expected.getRacy();
    Assert.assertEquals(racy.name(), actual.get(SafeSearchAnnotationSchema.RACY_FIELD_NAME));
  }
}
