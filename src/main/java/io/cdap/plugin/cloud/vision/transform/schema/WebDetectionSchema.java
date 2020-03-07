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
 * Relevant information for the image from the Internet.
 */
public class WebDetectionSchema {

  /**
   * Deduced entities from similar images on the Internet.
   */
  public static final String ENTITIES_FIELD_NAME = "webEntities";
  /**
   * Fully matching images from the Internet. Can include resized copies of the query image.
   */
  public static final String FULL_MATCHING_IMAGES_FIELD_NAME = "fullMatchingImages";
  /**
   * Partial matching images from the Internet. Those images are similar enough to share some key-point features.
   * For example an original image will likely have partial matching for its crops.
   */
  public static final String PARTIAL_MATCHING_IMAGES_FIELD_NAME = "partialMatchingImages";
  /**
   * Web pages containing the matching images from the Internet.
   */
  public static final String PAGES_WITH_MATCHING_IMAGES_FIELD_NAME = "pagesWithMatchingImages";
  /**
   * The visually similar image results.
   */
  public static final String VISUALLY_SIMILAR_IMAGES = "visuallySimilarImages";
  /**
   * The service's best guess as to the topic of the request image. Inferred from similar images on the open web.
   */
  public static final String BEST_GUESS_LABELS_FIELD_NAME = "bestGuessLabels";
  public static final Schema SCHEMA = Schema.recordOf(
          "web-detection-record",
          Schema.Field.of(ENTITIES_FIELD_NAME, Schema.arrayOf(WebEntity.SCHEMA)),
          Schema.Field.of(FULL_MATCHING_IMAGES_FIELD_NAME, Schema.arrayOf(WebImage.SCHEMA)),
          Schema.Field.of(PARTIAL_MATCHING_IMAGES_FIELD_NAME, Schema.arrayOf(WebImage.SCHEMA)),
          Schema.Field.of(PAGES_WITH_MATCHING_IMAGES_FIELD_NAME, Schema.arrayOf(WebPage.SCHEMA)),
          Schema.Field.of(VISUALLY_SIMILAR_IMAGES, Schema.arrayOf(WebImage.SCHEMA)),
          Schema.Field.of(BEST_GUESS_LABELS_FIELD_NAME, Schema.arrayOf(BestGuessLabel.SCHEMA))
  );

  private WebDetectionSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Entity deduced from similar images on the Internet.
   */
  public static class WebEntity {

    /**
     * Opaque entity ID.
     */
    public static final String ENTITY_ID_FIELD_NAME = "entityId";

    /**
     * Overall relevancy score for the entity. Not normalized and not comparable across different image queries.
     */
    public static final String SCORE_FIELD_NAME = "score";

    /**
     * Canonical description of the entity, in English.
     */
    public static final String DESCRIPTION_FIELD_NAME = "description";

    public static final Schema SCHEMA = Schema.recordOf(
            "web-entity-record",
            Schema.Field.of(ENTITY_ID_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(DESCRIPTION_FIELD_NAME, Schema.of(Schema.Type.STRING))
    );
  }

  /**
   * Metadata for web pages.
   */
  public static class WebPage {

    /**
     * The result web page URL.
     */
    public static final String URL_FIELD_NAME = "url";

    /**
     * Overall relevancy score for the web page.
     */
    public static final String SCORE_FIELD_NAME = "score";

    /**
     * Title for the web page, may contain HTML markups.
     */
    public static final String PAGE_TITLE_FIELD_NAME = "pageTitle";

    /**
     * Fully matching images on the page. Can include resized copies of the query image.
     */
    public static final String FULL_MATCHING_IMAGES_FIELD_NAME = "fullMatchingImages";

    /**
     * Partial matching images on the page. Those images are similar enough to share some key-point features.
     * For example an original image will likely have partial matching for its crops.
     */
    public static final String PARTIAL_MATCHING_IMAGES_FIELD_NAME = "partialMatchingImages";

    public static final Schema SCHEMA = Schema.recordOf(
            "page-with-matching-images-record",
            Schema.Field.of(URL_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(PAGE_TITLE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(FULL_MATCHING_IMAGES_FIELD_NAME, Schema.arrayOf(WebImage.SCHEMA)),
            Schema.Field.of(PARTIAL_MATCHING_IMAGES_FIELD_NAME, Schema.arrayOf(WebImage.SCHEMA))
    );
  }

  /**
   * Label to provide extra metadata for the web detection.
   */
  public static class BestGuessLabel {

    /**
     * Label for extra metadata.
     */
    public static final String LABEL_FIELD_NAME = "label";

    /**
     * The BCP-47 language code, such as "en-US" or "sr-Latn". For more information, see
     * http://www.unicode.org/reports/tr35/#Unicode_locale_identifier.
     */
    public static final String LANGUAGE_CODE_FIELD_NAME = "languageCode";

    public static final Schema SCHEMA = Schema.recordOf(
            "best-guess-label-record",
            Schema.Field.of(LABEL_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(LANGUAGE_CODE_FIELD_NAME, Schema.of(Schema.Type.STRING))
    );
  }

  /**
   * Metadata for online images.
   */
  public static class WebImage {

    /**
     * The result image URL.
     */
    public static final String URL_FIELD_NAME = "url";

    /**
     * Overall relevancy score for the image.
     */
    public static final String SCORE_FIELD_NAME = "score";

    public static final Schema SCHEMA = Schema.recordOf(
            "web-image-record",
            Schema.Field.of(URL_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(SCORE_FIELD_NAME, Schema.of(Schema.Type.FLOAT))
    );
  }
}
