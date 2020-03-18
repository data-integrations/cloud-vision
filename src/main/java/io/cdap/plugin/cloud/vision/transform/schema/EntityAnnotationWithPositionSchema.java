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
 * Landmark detection and Logo detection results mapped to a record with following fields.
 */
public class EntityAnnotationWithPositionSchema extends EntityAnnotationSchema {

  /**
   * Image region to which this entity belongs.
   */
  public static final String POSITION_FIELD_NAME = "position";
  public static final Schema SCHEMA = Schema.recordOf(
          "landmark-entity-annotation-component-record",
          Schema.Field.of(MID_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(LOCALE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(DESCRIPTION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(TOPICALITY_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(LOCATIONS_FIELD_NAME, Schema.arrayOf(LocationInfo.SCHEMA)),
          Schema.Field.of(POSITION_FIELD_NAME, Schema.nullableOf(
                  VertexSchema.getSchema("entityAnnotation-position"))),
          Schema.Field.of(PROPERTIES_FIELD_NAME, Schema.arrayOf(Property.SCHEMA))
  );

  private EntityAnnotationWithPositionSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
