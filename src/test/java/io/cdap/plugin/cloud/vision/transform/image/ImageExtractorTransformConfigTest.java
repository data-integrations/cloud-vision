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

package io.cdap.plugin.cloud.vision.transform.image;

import com.google.cloud.vision.v1.BoundingPoly;
import com.google.protobuf.util.JsonFormat;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.cloud.vision.ValidationAssertions;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfigTest;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConstants;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests of {@link ImageExtractorTransformConfig} methods.
 */
public class ImageExtractorTransformConfigTest extends ExtractorTransformConfigTest {

  @Override
  protected ImageExtractorTransformConfigBuilder getValidConfigBuilder() {
    return ImageExtractorTransformConfigBuilder.builder()
      .setPathField("path")
      .setOutputField("extracted")
      .setFeatures(ImageFeature.FACE.getDisplayName())
      .setSchema(VALID_SCHEMA.toString());
  }

  @Test
  public void testValidatePathFieldNull() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setPathField(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.PATH_FIELD);
  }

  @Test
  public void testValidatePathFieldEmpty() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setPathField("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.PATH_FIELD);
  }

  @Test
  public void testValidateBoundingPoly() throws Exception {
    String polyJson = "{ \"vertices\": [ " +
      "{ \"y\": 520 }, " +
      "{ \"x\": 2369, \"y\": 520 }, " +
      "{ \"x\": 2369, \"y\": 1729 }, " +
      "{ \"y\": 1729 } " +
      "] }";
    ImageExtractorTransformConfig config = getValidConfigBuilder()
      .setBoundingPolygon(polyJson)
      .build();

    BoundingPoly.Builder builder = BoundingPoly.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(polyJson, builder);
    BoundingPoly expectedBoundingPoly = builder.build();

    BoundingPoly actualBoundingPoly = config.getBoundingPoly();
    Assert.assertNotNull(actualBoundingPoly);
    Assert.assertEquals(expectedBoundingPoly, actualBoundingPoly);
  }

  @Test
  public void testValidateBoundingPolyInvalid() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setBoundingPolygon("invalid json")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.BOUNDING_POLYGON);
  }
}
