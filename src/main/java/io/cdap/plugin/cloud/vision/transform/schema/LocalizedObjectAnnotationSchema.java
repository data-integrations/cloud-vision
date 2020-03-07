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
 * Detected object with bounding boxes.
 */
public class LocalizedObjectAnnotationSchema {

  /**
   * Opaque entity ID. Some IDs may be available in
   * <a href="https://developers.google.com/knowledge-graph/">Google Knowledge Graph Search API</a>
   */
  public static final String MID_FIELD_NAME = "mid";
  /**
   * The BCP-47 language code, such as "en-US" or "sr-Latn". For more information, see
   * http://www.unicode.org/reports/tr35/#Unicode_locale_identifier.
   */
  public static final String LANGUAGE_CODE_FIELD_NAME = "languageCode";
  /**
   * Object name, expressed in its {@link LocalizedObjectAnnotationSchema#LANGUAGE_CODE_FIELD_NAME} language.
   */
  public static final String NAME_FIELD_NAME = "name";
  /**
   * Score of the result. Range [0, 1].
   */
  public static final String SCORE_FIELD_NAME = "score";
  /**
   * Image region to which this object belongs. This must be populated.
   */
  public static final String POSITION_FIELD_NAME = "position";
  public static final Schema SCHEMA = Schema.recordOf(
          "localized-object-annotation-component-record",
          Schema.Field.of(MID_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(LANGUAGE_CODE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(POSITION_FIELD_NAME, Schema.arrayOf(VertexSchema.SCHEMA))
  );

  protected LocalizedObjectAnnotationSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
