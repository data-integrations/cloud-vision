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
 * Text annotation is mapped to a record with following fields.
 */
public class TextAnnotationSchema {

  /**
   * The language code for the locale in which the entity textual {@value TextAnnotationSchema#DESCRIPTION_FIELD_NAME}
   * is expressed.
   */
  public static final String LOCALE_FIELD_NAME = "locale";
  /**
   * Entity textual description, expressed in its {@value TextAnnotationSchema#LOCALE_FIELD_NAME} language.
   */
  public static final String DESCRIPTION_FIELD_NAME = "description";
  /**
   * Image region to which this entity belongs.
   */
  public static final String POSITION_FIELD_NAME = "position";
  public static final Schema SCHEMA = Schema.recordOf(
          "text-annotation-component-record",
          Schema.Field.of(LOCALE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(DESCRIPTION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(POSITION_FIELD_NAME, Schema.arrayOf(VertexSchema.SCHEMA)));

  private TextAnnotationSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
