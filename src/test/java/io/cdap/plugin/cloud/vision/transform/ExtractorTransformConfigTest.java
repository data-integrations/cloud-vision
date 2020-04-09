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

package io.cdap.plugin.cloud.vision.transform;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.cloud.vision.ValidationAssertions;
import org.junit.Test;

/**
 * Tests of {@link ExtractorTransformConfig} methods.
 */
public abstract class ExtractorTransformConfigTest {

  protected static final String MOCK_STAGE = "mockstage";
  protected static final Schema VALID_SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("extracted", ImageFeature.FACE.getSchema())
  );

  protected abstract ExtractorTransformConfigBuilder getValidConfigBuilder();

  @Test
  public void testValidateOutputFieldNull() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setOutputField(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.OUTPUT_FIELD);
  }

  @Test
  public void testValidateOutputFieldEmpty() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setOutputField("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.OUTPUT_FIELD);
  }

  @Test
  public void testValidateFeatureNull() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setFeatures(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.FEATURES);
  }

  @Test
  public void testValidateFeatureEmpty() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setFeatures("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.FEATURES);
  }

  @Test
  public void testValidateFeatureInvalid() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setFeatures("invalid-split-by")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ExtractorTransformConstants.FEATURES);
  }
}
