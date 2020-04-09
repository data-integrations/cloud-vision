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
 * Label detection results mapped to a record with following fields.
 */
public class EntityAnnotationSchema {

  protected EntityAnnotationSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Opaque entity ID. Some IDs may be available in
   * <a href="https://developers.google.com/knowledge-graph/">Google Knowledge Graph Search API</a>
   */
  public static final String MID_FIELD_NAME = "mid";

  /**
   * The language code for the locale in which the entity textual description is expressed.
   */
  public static final String LOCALE_FIELD_NAME = "locale";

  /**
   * Entity textual description.
   */
  public static final String DESCRIPTION_FIELD_NAME = "description";

  /**
   * Overall score of the result. Range [0, 1].
   */
  public static final String SCORE_FIELD_NAME = "score";

  /**
   * The relevancy of the ICA (Image Content Annotation) label to the image. For example, the relevancy of "tower" is
   * likely higher to an image containing the detected "Eiffel Tower" than to an image containing a detected distant
   * towering building, even though the confidence that there is a tower in each image may be the same. Range [0, 1].
   */
  public static final String TOPICALITY_FIELD_NAME = "topicality";

  /**
   * The location information for the detected entity. Multiple location elements can be present because one
   * location may indicate the location of the scene in the image, and another location may indicate the location of
   * the place where the image was taken. Location information is usually present for landmarks.
   */
  public static final String LOCATIONS_FIELD_NAME = "locations";

  /**
   * Some entities may have optional user-supplied Property (name/value) fields, such a score or string that qualifies
   * the entity.
   */
  public static final String PROPERTIES_FIELD_NAME = "properties";

  public static final Schema SCHEMA = Schema.recordOf(
    "label-entity-annotation-component-record",
    Schema.Field.of(MID_FIELD_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(LOCALE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(DESCRIPTION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(TOPICALITY_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(LOCATIONS_FIELD_NAME, Schema.arrayOf(LocationInfo.SCHEMA)),
    Schema.Field.of(PROPERTIES_FIELD_NAME, Schema.arrayOf(Property.SCHEMA))
  );

  /**
   * Detected entity location information.
   */
  public static class LocationInfo {

    /**
     * Latitude.
     */
    public static final String LATITUDE_FIELD_NAME = "latitude";

    /**
     * Longitude.
     */
    public static final String LONGITUDE_FIELD_NAME = "longitude";

    public static final Schema SCHEMA = Schema.recordOf(
      "location-info-record",
      Schema.Field.of(LATITUDE_FIELD_NAME, Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of(LONGITUDE_FIELD_NAME, Schema.of(Schema.Type.DOUBLE))
    );
  }

  /**
   * A Property consists of a user-supplied name/value pair.
   */
  public static class Property {

    /**
     * Name of the property.
     */
    public static final String NAME_FIELD_NAME = "name";

    /**
     * Value of the property.
     */
    public static final String VALUE_FIELD_NAME = "value";

    /**
     * Value of numeric properties.
     */
    public static final String UINT_64_VALUE_FIELD_NAME = "uint64Value";

    public static final Schema SCHEMA = Schema.recordOf(
      "property-record",
      Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(VALUE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(UINT_64_VALUE_FIELD_NAME, Schema.of(Schema.Type.LONG))
    );
  }
}
