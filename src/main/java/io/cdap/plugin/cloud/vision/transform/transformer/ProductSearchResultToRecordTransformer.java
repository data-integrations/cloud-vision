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

import com.google.cloud.Timestamp;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.Product;
import com.google.cloud.vision.v1.ProductSearchResults;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.ProductSearchResultsSchema;

import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms product search results of specified {@link AnnotateImageResponse} to {@link StructuredRecord}
 * according to the specified schema.
 */
public class ProductSearchResultToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public ProductSearchResultToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    ProductSearchResults productSearchResults = annotateImageResponse.getProductSearchResults();
    return getOutputRecordBuilder(input)
            .set(outputFieldName, extractProductSearchResults(productSearchResults))
            .build();
  }

  private StructuredRecord extractProductSearchResults(ProductSearchResults searchResults) {
    Schema schema = getProductSearchResultSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(ProductSearchResultsSchema.INDEX_TIME_FIELD_NAME) != null && searchResults.hasIndexTime()) {
      // Timestamp in RFC3339 UTC "Zulu" format, accurate to nanoseconds. Example: "2014-10-02T15:01:23.045123456Z".
      // Mapped to string to avoid accuracy loss since CDAP timestamp accurate to microseconds
      Timestamp indexTimestamp = Timestamp.fromProto(searchResults.getIndexTime());
      builder.set(ProductSearchResultsSchema.INDEX_TIME_FIELD_NAME, indexTimestamp.toString());
    }
    Schema.Field resultsField = schema.getField(ProductSearchResultsSchema.RESULTS_FIELD_NAME);
    if (resultsField != null) {
      Schema resultSchema = getComponentSchema(resultsField);
      List<StructuredRecord> results = searchResults.getResultsList().stream()
              .map(r -> extractProductSearchResultRecord(r, resultSchema))
              .collect(Collectors.toList());
      builder.set(ProductSearchResultsSchema.RESULTS_FIELD_NAME, results);
    }
    Schema.Field groupedResField = schema.getField(ProductSearchResultsSchema.GROUPED_RESULTS_FIELD_NAME);
    if (groupedResField != null) {
      Schema resultSchema = getComponentSchema(groupedResField);
      List<StructuredRecord> results = searchResults.getProductGroupedResultsList().stream()
              .map(r -> extractProductSearchResultRecord(r, resultSchema))
              .collect(Collectors.toList());
      builder.set(ProductSearchResultsSchema.GROUPED_RESULTS_FIELD_NAME, results);
    }

    return builder.build();
  }

  private StructuredRecord extractProductSearchResultRecord(ProductSearchResults.Result result, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(ProductSearchResultsSchema.Result.IMAGE_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.Result.IMAGE_FIELD_NAME, result.getImage());
    }
    if (schema.getField(ProductSearchResultsSchema.Result.SCORE_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.Result.SCORE_FIELD_NAME, result.getScore());
    }
    Schema.Field productField = schema.getField(ProductSearchResultsSchema.Result.PRODUCT_FIELD_NAME);
    if (productField != null) {
      Schema productSchema = productField.getSchema();
      Schema nonNullableProductSchema = productSchema.isNullable() ? productSchema.getNonNullable() : productSchema;
      StructuredRecord product = extractProductRecord(result.getProduct(), nonNullableProductSchema);
      builder.set(ProductSearchResultsSchema.Result.PRODUCT_FIELD_NAME, product);
    }

    return builder.build();
  }

  private StructuredRecord extractProductRecord(Product product, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(ProductSearchResultsSchema.Product.NAME_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.Product.NAME_FIELD_NAME, product.getName());
    }
    if (schema.getField(ProductSearchResultsSchema.Product.DISPLAY_NAME_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.Product.DISPLAY_NAME_FIELD_NAME, product.getDisplayName());
    }
    if (schema.getField(ProductSearchResultsSchema.Product.DESCRIPTION_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.Product.DESCRIPTION_FIELD_NAME, product.getDescription());
    }
    if (schema.getField(ProductSearchResultsSchema.Product.PRODUCT_CATEGORY_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.Product.PRODUCT_CATEGORY_FIELD_NAME, product.getProductCategory());
    }
    Schema.Field labelsField = schema.getField(ProductSearchResultsSchema.Product.PRODUCT_LABELS_FIELD_NAME);
    if (labelsField != null) {
      Schema labelSchema = getComponentSchema(labelsField);
      List<StructuredRecord> labels = product.getProductLabelsList().stream()
              .map(label -> extractProductLabelRecord(label, labelSchema))
              .collect(Collectors.toList());
      builder.set(ProductSearchResultsSchema.Product.PRODUCT_LABELS_FIELD_NAME, labels);
    }

    return builder.build();
  }

  private StructuredRecord extractProductLabelRecord(Product.KeyValue label, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(ProductSearchResultsSchema.KeyValue.KEY_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.KeyValue.KEY_FIELD_NAME, label.getKey());
    }
    if (schema.getField(ProductSearchResultsSchema.KeyValue.VALUE_FIELD_NAME) != null) {
      builder.set(ProductSearchResultsSchema.KeyValue.VALUE_FIELD_NAME, label.getValue());
    }
    return builder.build();
  }

  private StructuredRecord extractProductSearchResultRecord(ProductSearchResults.GroupedResult result, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Schema.Field positionField = schema.getField(ProductSearchResultsSchema.GroupedResult.POSITION_FIELD_NAME);
    if (positionField != null) {
      Schema positionSchema = getComponentSchema(positionField);
      List<StructuredRecord> position = result.getBoundingPoly().getVerticesList().stream()
              .map(vertex -> extractVertex(vertex, positionSchema))
              .collect(Collectors.toList());
      builder.set(ProductSearchResultsSchema.GroupedResult.POSITION_FIELD_NAME, position);
    }
    Schema.Field resultsField = schema.getField(ProductSearchResultsSchema.GroupedResult.RESULTS_FIELD_NAME);
    if (positionField != null) {
      Schema resultSchema = getComponentSchema(resultsField);
      List<StructuredRecord> results = result.getResultsList().stream()
              .map(r -> extractProductSearchResultRecord(r, resultSchema))
              .collect(Collectors.toList());
      builder.set(ProductSearchResultsSchema.GroupedResult.RESULTS_FIELD_NAME, results);
    }

    return builder.build();
  }

  /**
   * Retrieves Product Search Result non-nullable component schema. Schema retrieved instead of using constant
   * schema since users are free to choose to not include some of the fields.
   *
   * @return Product Search Result non-nullable component schema.
   */
  private Schema getProductSearchResultSchema() {
    Schema productSearchResultsFieldSchema = schema.getField(outputFieldName).getSchema();
    return productSearchResultsFieldSchema.isNullable() ? productSearchResultsFieldSchema.getNonNullable()
            : productSearchResultsFieldSchema;
  }
}
