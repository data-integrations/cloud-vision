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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.cloud.vision.CloudVisionConstants;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import org.junit.Assert;
import org.junit.Test;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test class for {@link OfflineImageExtractorActionConfig}.
 */
public class OfflineImageExtractorActionConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final OfflineImageExtractorActionConfig VALID_CONFIG = new OfflineImageExtractorActionConfig(
    CloudVisionConstants.AUTO_DETECT,
    ImageFeature.FACE.getDisplayName(),
    null,
    null,
    true,
    "sourcePath",
    "destinationPath",
    "2"
  );

  @Test
  public void testCheckValidConfig() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(collector);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateIncorrectBatchSize() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(VALID_CONFIG)
      .setBatchSize("1t")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(ActionConstants.BATCH_SIZE)
    );

    config.validate(collector);
    assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateNegativeBatchSize() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(VALID_CONFIG)
      .setBatchSize("-1")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(ActionConstants.BATCH_SIZE)
    );

    config.validate(collector);
    assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidatePositiveBatchSize() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(VALID_CONFIG)
      .setBatchSize("102")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(ActionConstants.BATCH_SIZE)
    );

    config.validate(collector);
    assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateCorrectBatchSize() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(VALID_CONFIG)
      .setBatchSize("20")
      .build();

    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateAspectRatios() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(VALID_CONFIG)
      .setFeatures(ImageFeature.CROP_HINTS.getDisplayName())
      .setAspectRatios("1t")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(ActionConstants.ASPECT_RATIOS)
    );

    config.validate(collector);
    assertValidationFailed(collector, paramNames);
  }

  @Test
  public void testValidateOneLanguageHint() {
    // Those are all the language ids retrieved from 'widgets/DocumentExtractor-transform.json'
    String[] languages = new String[]{"af", "sq", "ar", "hy", "be", "bn", "bg", "ca", "zh", "hr", "cs", "da", "nl",
      "en", "et", "fil", "fi", "fr", "de", "el", "gu", "iw", "hu", "is", "id", "it", "ja", "kn", "km", "ko", "lo",
      "lv", "lt", "mk", "ms", "ml", "mr", "ne", "no", "fa", "pl", "pt", "pa", "ro", "ru", "ru-PETR1708", "sr",
      "sr-Latn", "sk", "sl", "es", "sv", "ta", "te", "th", "tr", "uk", "vi", "yi"};

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(ActionConstants.LANGUAGE_HINTS)
    );

    // Validate the languages one by one
    for (String language : languages) {
      OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(VALID_CONFIG)
        .setFeatures(ImageFeature.TEXT.getDisplayName())
        .setLanguageHints(language)
        .build();
      config.validate(collector);
    }
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  private void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
