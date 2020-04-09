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

package io.cdap.plugin.cloud.vision.transform.schema;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Set of features pertaining to the image, computed by computer vision methods over safe-search verticals
 * (for example, adult, spoof, medical, violence).
 */
public class SafeSearchAnnotationSchema {

  private SafeSearchAnnotationSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Represents the adult content likelihood for the image. Adult content may contain elements such as nudity,
   * pornographic images or cartoons, or sexual activities.
   */
  public static final String ADULT_FIELD_NAME = "adult";

  /**
   * Spoof likelihood. The likelihood that an modification was made to the image's canonical version to make it appear
   * funny or offensive.
   */
  public static final String SPOOF_FIELD_NAME = "spoof";

  /**
   * Likelihood that this is a medical image.
   */
  public static final String MEDICAL_FIELD_NAME = "medical";

  /**
   * Likelihood that this image contains violent content.
   */
  public static final String VIOLENCE_FIELD_NAME = "violence";

  /**
   * Likelihood that the request image contains racy content. Racy content may include (but is not limited to) skimpy
   * or sheer clothing, strategically covered nudity, lewd or provocative poses, or close-ups of sensitive body areas.
   */
  public static final String RACY_FIELD_NAME = "racy";

  public static final Schema SCHEMA = Schema.recordOf(
    "safe-search-annotation-record",
    Schema.Field.of(ADULT_FIELD_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(SPOOF_FIELD_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(MEDICAL_FIELD_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(VIOLENCE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(RACY_FIELD_NAME, Schema.of(Schema.Type.STRING))
  );
}
