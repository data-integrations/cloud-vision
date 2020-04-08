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

package io.cdap.plugin.cloud.vision.transform.transformer;

import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.WebDetection;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.WebDetectionSchema;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transforms web detection of specified {@link AnnotateImageResponse} to {@link StructuredRecord}
 * according to the specified schema.
 */
public class WebDetectionToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public WebDetectionToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link WebDetectionSchema}. This {@link StructuredRecord} can then be turned into a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    WebDetection webDetection = annotateImageResponse.getWebDetection();
    return getOutputRecordBuilder(input)
        .set(outputFieldName, extractWebDetection(webDetection))
        .build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the Web detection information from a
   * {@link WebDetection} input using a {@link Schema} for the mapping.
   *
   * @param webDetection The {@link WebDetection} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractWebDetection(WebDetection webDetection) {
    Schema webSchema = getWebDetectionSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(webSchema);

    Schema.Field entitiesField = webSchema.getField(WebDetectionSchema.ENTITIES_FIELD_NAME);
    if (entitiesField != null) {
      Schema entitySchema = getComponentSchema(entitiesField);
      List<StructuredRecord> entities = webDetection.getWebEntitiesList().stream()
          .map(v -> extractEntity(v, entitySchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.ENTITIES_FIELD_NAME, entities);
    }

    Schema.Field fullMatchingImagesField = webSchema.getField(WebDetectionSchema.FULL_MATCHING_IMAGES_FIELD_NAME);
    if (fullMatchingImagesField != null) {
      Schema webImageSchema = getComponentSchema(fullMatchingImagesField);
      List<StructuredRecord> webImages = webDetection.getFullMatchingImagesList().stream()
          .map(v -> extractWebImage(v, webImageSchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.FULL_MATCHING_IMAGES_FIELD_NAME, webImages);
    }

    Schema.Field partialMatchingImagesField = webSchema.getField(WebDetectionSchema.PARTIAL_MATCHING_IMAGES_FIELD_NAME);
    if (partialMatchingImagesField != null) {
      Schema webImageSchema = getComponentSchema(partialMatchingImagesField);
      List<StructuredRecord> webImages = webDetection.getPartialMatchingImagesList().stream()
          .map(v -> extractWebImage(v, webImageSchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.PARTIAL_MATCHING_IMAGES_FIELD_NAME, webImages);
    }

    Schema.Field pagesField = webSchema.getField(WebDetectionSchema.PAGES_WITH_MATCHING_IMAGES_FIELD_NAME);
    if (pagesField != null) {
      Schema pageSchema = getComponentSchema(pagesField);
      List<StructuredRecord> pages = webDetection.getPagesWithMatchingImagesList().stream()
          .map(v -> extractWebPage(v, pageSchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.PAGES_WITH_MATCHING_IMAGES_FIELD_NAME, pages);
    }

    Schema.Field visuallySimilarImagesField = webSchema.getField(WebDetectionSchema.VISUALLY_SIMILAR_IMAGES);
    if (visuallySimilarImagesField != null) {
      Schema webImageSchema = getComponentSchema(visuallySimilarImagesField);
      List<StructuredRecord> webImages = webDetection.getVisuallySimilarImagesList().stream()
          .map(v -> extractWebImage(v, webImageSchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.VISUALLY_SIMILAR_IMAGES, webImages);
    }
    Schema.Field labelsField = webSchema.getField(WebDetectionSchema.BEST_GUESS_LABELS_FIELD_NAME);
    if (labelsField != null) {
      Schema labelSchema = getComponentSchema(labelsField);
      List<StructuredRecord> webLabels = webDetection.getBestGuessLabelsList().stream()
          .map(v -> extractWebLabel(v, labelSchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.BEST_GUESS_LABELS_FIELD_NAME, webLabels);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the Web entity information from a
   * {@link WebDetection.WebEntity} input using a {@link Schema} for the mapping.
   *
   * @param webEntity The {@link WebDetection.WebEntity} object containing the data.
   * @param schema    The {@link Schema} to use.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractEntity(WebDetection.WebEntity webEntity, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(WebDetectionSchema.WebEntity.ENTITY_ID_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebEntity.ENTITY_ID_FIELD_NAME, webEntity.getEntityId());
    }
    if (schema.getField(WebDetectionSchema.WebEntity.SCORE_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebEntity.SCORE_FIELD_NAME, webEntity.getScore());
    }
    if (schema.getField(WebDetectionSchema.WebEntity.DESCRIPTION_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebEntity.DESCRIPTION_FIELD_NAME, webEntity.getDescription());
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the Web image information from a
   * {@link WebDetection.WebImage} input using a {@link Schema} for the mapping.
   *
   * @param webImage The {@link WebDetection.WebImage} object containing the data.
   * @param schema   The {@link Schema} to use.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractWebImage(WebDetection.WebImage webImage, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(WebDetectionSchema.WebImage.URL_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebImage.URL_FIELD_NAME, webImage.getUrl());
    }
    if (schema.getField(WebDetectionSchema.WebImage.SCORE_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebImage.SCORE_FIELD_NAME, webImage.getScore());
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the Web label information from a
   * {@link WebDetection.WebLabel} input using a {@link Schema} for the mapping.
   *
   * @param webLabel The {@link WebDetection.WebLabel} object containing the data.
   * @param schema   The {@link Schema} to use.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractWebLabel(WebDetection.WebLabel webLabel, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(WebDetectionSchema.BestGuessLabel.LABEL_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.BestGuessLabel.LABEL_FIELD_NAME, webLabel.getLabel());
    }
    if (schema.getField(WebDetectionSchema.BestGuessLabel.LANGUAGE_CODE_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.BestGuessLabel.LANGUAGE_CODE_FIELD_NAME, webLabel.getLanguageCode());
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the Web page information from a
   * {@link WebDetection.WebPage} input using a {@link Schema} for the mapping.
   *
   * @param webPage The {@link WebDetection.WebPage} object containing the data.
   * @param schema  The {@link Schema} to use.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractWebPage(WebDetection.WebPage webPage, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(WebDetectionSchema.WebPage.URL_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebPage.URL_FIELD_NAME, webPage.getUrl());
    }
    if (schema.getField(WebDetectionSchema.WebPage.PAGE_TITLE_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebPage.PAGE_TITLE_FIELD_NAME, webPage.getPageTitle());
    }
    if (schema.getField(WebDetectionSchema.WebPage.SCORE_FIELD_NAME) != null) {
      builder.set(WebDetectionSchema.WebPage.SCORE_FIELD_NAME, webPage.getScore());
    }

    Schema.Field fullMatchingImgField = schema.getField(WebDetectionSchema.WebPage.FULL_MATCHING_IMAGES_FIELD_NAME);
    if (fullMatchingImgField != null) {
      Schema webImageSchema = getComponentSchema(fullMatchingImgField);
      List<StructuredRecord> webImages = webPage.getFullMatchingImagesList().stream()
          .map(v -> extractWebImage(v, webImageSchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.WebPage.FULL_MATCHING_IMAGES_FIELD_NAME, webImages);
    }

    Schema.Field partMatchingImgField = schema.getField(WebDetectionSchema.WebPage.PARTIAL_MATCHING_IMAGES_FIELD_NAME);
    if (partMatchingImgField != null) {
      Schema webImageSchema = getComponentSchema(partMatchingImgField);
      List<StructuredRecord> webImages = webPage.getPartialMatchingImagesList().stream()
          .map(v -> extractWebImage(v, webImageSchema))
          .collect(Collectors.toList());
      builder.set(WebDetectionSchema.WebPage.PARTIAL_MATCHING_IMAGES_FIELD_NAME, webImages);
    }

    return builder.build();
  }

  /**
   * Retrieves Web Detection's non-nullable component schema. Schema retrieved instead of using constant schema
   * since users are free to choose to not include some of the fields
   *
   * @return Web Detection's non-nullable component schema.
   */
  private Schema getWebDetectionSchema() {
    Schema webDetectionAnnotationsFieldSchema = schema.getField(outputFieldName).getSchema();
    return webDetectionAnnotationsFieldSchema.isNullable() ? webDetectionAnnotationsFieldSchema.getNonNullable()
        : webDetectionAnnotationsFieldSchema;
  }
}
