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

package io.cdap.plugin.cloud.vision.action;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.cloud.vision.CloudVisionConstants;
import io.cdap.plugin.cloud.vision.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link TextExtractorActionConfigTest}.
 */
public class TextExtractorActionConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final TextExtractorActionConfig VALID_CONFIG = new TextExtractorActionConfig(
          "/path",
          "/path",
          "/path",
          "application/pdf",
          2,
          null
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testEmptyServiceFilePath() {
    TextExtractorActionConfig config = TextExtractorActionConfig.builder(VALID_CONFIG)
            .setServiceFilePath("")
            .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector,
            CloudVisionConstants.SERVICE_ACCOUNT_FILE_PATH);
  }

  @Test
  public void testEmptySourcePath() {
    TextExtractorActionConfig config = TextExtractorActionConfig.builder(VALID_CONFIG)
            .setSourcePath("")
            .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ActionConstants.SOURCE_PATH);
  }

  @Test
  public void testEmptyDestinationPath() {
    TextExtractorActionConfig config = TextExtractorActionConfig.builder(VALID_CONFIG)
            .setDestinationPath("")
            .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ActionConstants.DESTINATION_PATH);
  }
}
