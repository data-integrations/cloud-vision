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
import com.google.cloud.vision.v1.ColorInfo;
import com.google.cloud.vision.v1.DominantColorsAnnotation;
import com.google.cloud.vision.v1.ImageProperties;
import com.google.type.Color;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.schema.ColorInfoSchema;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;

/**
 * {@link ImagePropertiesAnnotationsToRecordTransformer} test.
 */
public class ImagePropertiesAnnotationsToRecordTransformerTest extends BaseAnnotationsToRecordTransformerTest {

  private static final ColorInfo COLOR_1 = ColorInfo.newBuilder()
    .setPixelFraction(0.18f)
    .setScore(0.18f)
    .setColor(Color.newBuilder().setRed(204).setGreen(205).setBlue(213))
    .build();

  private static final ColorInfo COLOR_2 = ColorInfo.newBuilder()
    .setPixelFraction(0.38f)
    .setScore(0.98f)
    .setColor(Color.newBuilder().setRed(204).setGreen(205).setBlue(213))
    .build();

  private static final DominantColorsAnnotation DOMINANT_COLORS_ANNOTATION = DominantColorsAnnotation.newBuilder()
    .addAllColors(Arrays.asList(COLOR_1, COLOR_2))
    .build();

  private static final ImageProperties IMAGE_PROPERTIES = ImageProperties.newBuilder()
    .setDominantColors(DOMINANT_COLORS_ANNOTATION)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .setImagePropertiesAnnotation(IMAGE_PROPERTIES)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.IMAGE_PROPERTIES.getSchema()));

    ImagePropertiesAnnotationsToRecordTransformer transformer =
      new ImagePropertiesAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    List<StructuredRecord> actual = transformed.get(output);
    assertAnnotationEquals(DOMINANT_COLORS_ANNOTATION, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.IMAGE_PROPERTIES.getSchema()));

    ImagePropertiesAnnotationsToRecordTransformer transformer =
      new ImagePropertiesAnnotationsToRecordTransformer(schema, output);

    DominantColorsAnnotation emptyAnnotation = DominantColorsAnnotation.newBuilder().build();
    AnnotateImageResponse emptyTextAnnotation = AnnotateImageResponse.newBuilder()
      .setImagePropertiesAnnotation(ImageProperties.newBuilder().setDominantColors(emptyAnnotation).build())
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
      "single-color-info-field",
      Schema.Field.of(ColorInfoSchema.SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)));
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, Schema.arrayOf(textAnnotationSingleFieldSchema)));

    ImagePropertiesAnnotationsToRecordTransformer transformer =
      new ImagePropertiesAnnotationsToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);
    Assert.assertNotNull(transformed);
    List<StructuredRecord> actual = transformed.get(output);

    Assert.assertNotNull(actual);
    Assert.assertEquals(DOMINANT_COLORS_ANNOTATION.getColorsList().size(), actual.size());
    for (int i = 0; i < DOMINANT_COLORS_ANNOTATION.getColorsList().size(); i++) {
      ColorInfo expectedColorInfo = DOMINANT_COLORS_ANNOTATION.getColors(i);
      StructuredRecord actualColorInfo = actual.get(i);
      Assert.assertEquals(expectedColorInfo.getScore(),
        actualColorInfo.<Float>get(ColorInfoSchema.SCORE_FIELD_NAME),
        DELTA);
    }
  }

  private void assertAnnotationEquals(DominantColorsAnnotation expected, List<StructuredRecord> actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getColorsList().size(), actual.size());
    for (int i = 0; i < expected.getColorsList().size(); i++) {
      ColorInfo expectedColor = expected.getColors(i);
      StructuredRecord actualColorInfo = actual.get(i);
      assertColorEquals(expectedColor, actualColorInfo);
    }
  }

  private void assertColorEquals(ColorInfo expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getScore(), actual.<Float>get(ColorInfoSchema.SCORE_FIELD_NAME), DELTA);
    Assert.assertEquals(expected.getColor().getRed(), actual.<Float>get(ColorInfoSchema.RED_FIELD_NAME), DELTA);
    Assert.assertEquals(expected.getColor().getGreen(), actual.<Float>get(ColorInfoSchema.GREEN_FIELD_NAME), DELTA);
    Assert.assertEquals(expected.getColor().getBlue(), actual.<Float>get(ColorInfoSchema.BLUE_FIELD_NAME), DELTA);
    Assert.assertEquals(expected.getPixelFraction(),
      actual.<Float>get(ColorInfoSchema.PIXEL_FRACTION_FIELD_NAME),
      DELTA);
    Assert.assertEquals(expected.getColor().getAlpha().getValue(),
      actual.<Float>get(ColorInfoSchema.ALPHA_FIELD_NAME),
      DELTA);
  }
}
