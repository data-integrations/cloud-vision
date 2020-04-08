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
 * A vertex represents a 2D point in the image. {@link com.google.cloud.vision.v1.Vertex} mapped to
 * a record with the following fields.
 */
public class VertexSchema {

  /**
   * X coordinate.
   */

  public static final String X_FIELD_NAME = "x";
  /**
   * Y coordinate.
   */
  public static final String Y_FIELD_NAME = "y";

  /**
   * Utility method to create a {@link Schema} with a specific name. This is useful to create uniquely named schemas
   * that will be combined into a larger {@link Schema}.
   *
   * @param name {@link String} containing the name to give to the returned {@link Schema}.
   * @return a {@link Schema} with the given name.
   */
  public static Schema getSchema(String name) {
    return Schema.recordOf(name,
        Schema.Field.of(X_FIELD_NAME, Schema.of(Schema.Type.INT)),
        Schema.Field.of(Y_FIELD_NAME, Schema.of(Schema.Type.INT)));
  }

  /*
   * Prevent instantiating this class.
   */
  private VertexSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
