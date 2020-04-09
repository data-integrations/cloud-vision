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

package io.cdap.plugin.cloud.vision.transform.document;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.cloud.vision.ValidationAssertions;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfigTest;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/**
 * Tests of {@link DocumentExtractorTransformConfig} methods.
 */
public class DocumentExtractorTransformConfigTest extends ExtractorTransformConfigTest {

  @Override
  protected DocumentExtractorTransformConfigBuilder getValidConfigBuilder() {
    return DocumentExtractorTransformConfigBuilder.builder()
      .setPathField("path")
      .setOutputField("extracted")
      .setFeatures(ImageFeature.FACE.getDisplayName())
      .setMimeType("application/pdf")
      .setSchema(VALID_SCHEMA.toString());
  }

  @Test
  public void testValidatePathFieldAndContentFieldNotSet() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setPathField(null)
      .setContentField(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = ValidationAssertions.getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(2, causeList.size());
    ValidationFailure.Cause firstCause = causeList.get(0);
    Assert.assertEquals(DocumentExtractorTransformConstants.PATH_FIELD,
      firstCause.getAttribute(CauseAttributes.STAGE_CONFIG));
    ValidationFailure.Cause secondCause = causeList.get(1);
    Assert.assertEquals(DocumentExtractorTransformConstants.CONTENT_FIELD,
      secondCause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateValidOnlyPathFieldSet() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setPathField("path")
      .setContentField(null)
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateValidOnlyContentFieldSet() {
    ExtractorTransformConfig config = getValidConfigBuilder()
      .setPathField(null)
      .setContentField("content")
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateMimeTypeNull() {
    DocumentExtractorTransformConfig config = getValidConfigBuilder()
      .setMimeType(null)
      .build();

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    ValidationAssertions.assertPropertyValidationFailed(collector, DocumentExtractorTransformConstants.MIME_TYPE);
  }

  @Test
  public void testValidateMimeTypeEmpty() {
    DocumentExtractorTransformConfig config = getValidConfigBuilder()
      .setMimeType("")
      .build();

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    ValidationAssertions.assertPropertyValidationFailed(collector, DocumentExtractorTransformConstants.MIME_TYPE);
  }
}
