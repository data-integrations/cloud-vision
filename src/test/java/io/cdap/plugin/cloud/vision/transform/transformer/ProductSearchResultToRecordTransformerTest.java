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
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.cloud.vision.transform.ProductCategory;
import io.cdap.plugin.cloud.vision.transform.schema.ProductSearchResultsSchema;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/**
 * {@link ProductSearchResultToRecordTransformer} test.
 */
public class ProductSearchResultToRecordTransformerTest extends BaseAnnotationsToRecordTransformerTest {

  private static final ProductSearchResults.Result RESULT = ProductSearchResults.Result.newBuilder()
    .setImage("gs://product-search-tutorial/dress-shoe-dataset/469a896b70ba11e8be97d20059124800.jpg")
    .setScore(0.5f)
    .setProduct(Product.newBuilder()
      .setName("projects/prj-prod-search-tutorials/locations/us-east1/products/P_CLOTH-SHOE_46903668_070318")
      .setDisplayName("Blue Dress")
      .setProductCategory(ProductCategory.APPAREL.getName())
      .setDescription("Short sleeved and 1950s style satin dress")
      .addProductLabels(Product.KeyValue.newBuilder().setKey("color").setValue("blue").build())
      .build())
    .build();

  private static final ProductSearchResults.GroupedResult GROUPED = ProductSearchResults.GroupedResult.newBuilder()
    .setBoundingPoly(POSITION)
    .addResults(RESULT)
    .build();

  private static final ProductSearchResults PRODUCT_SEARCH_RESULTS = ProductSearchResults.newBuilder()
    .setIndexTime(Timestamp.parseTimestamp("2018-10-02T15:01:23.045123456Z").toProto())
    .addResults(RESULT)
    .addProductGroupedResults(GROUPED)
    .build();

  private static final AnnotateImageResponse RESPONSE = AnnotateImageResponse.newBuilder()
    .setProductSearchResults(PRODUCT_SEARCH_RESULTS)
    .build();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.PRODUCT_SEARCH.getSchema()));

    ProductSearchResultToRecordTransformer transformer = new ProductSearchResultToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    StructuredRecord actual = transformed.get(output);
    assertProductSearchResultsEquals(PRODUCT_SEARCH_RESULTS, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyAnnotation() {
    String output = "extracted";
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, ImageFeature.PRODUCT_SEARCH.getSchema()));

    ProductSearchResultToRecordTransformer transformer = new ProductSearchResultToRecordTransformer(schema, output);

    ProductSearchResults emptyResults = ProductSearchResults.newBuilder().build();
    AnnotateImageResponse response = AnnotateImageResponse.newBuilder()
      .setProductSearchResults(emptyResults)
      .build();
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, response);

    Assert.assertNotNull(transformed);
    StructuredRecord actual = transformed.get(output);
    assertProductSearchResultsEquals(emptyResults, actual);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformSingleField() {
    String output = "extracted";
    Schema singleFieldSchema = Schema.recordOf(
      "single-field",
      Schema.Field.of(ProductSearchResultsSchema.INDEX_TIME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("transformed-record-schema",
      Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(output, singleFieldSchema));

    ProductSearchResultToRecordTransformer transformer = new ProductSearchResultToRecordTransformer(schema, output);
    StructuredRecord transformed = transformer.transform(INPUT_RECORD, RESPONSE);

    Assert.assertNotNull(transformed);
    StructuredRecord actual = transformed.get(output);
    // actual record has single-field schema
    Assert.assertEquals(singleFieldSchema, actual.getSchema());
    Assert.assertEquals(Timestamp.fromProto(PRODUCT_SEARCH_RESULTS.getIndexTime()).toString(),
      actual.get(ProductSearchResultsSchema.INDEX_TIME_FIELD_NAME));
  }

  private void assertProductSearchResultsEquals(ProductSearchResults expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    String expectedTimestamp = expected.hasIndexTime() ? Timestamp.fromProto(expected.getIndexTime()).toString() : null;
    Assert.assertEquals(expectedTimestamp, actual.get(ProductSearchResultsSchema.INDEX_TIME_FIELD_NAME));

    List<StructuredRecord> results = actual.get(ProductSearchResultsSchema.RESULTS_FIELD_NAME);
    Assert.assertNotNull(results);
    Assert.assertEquals(expected.getResultsCount(), results.size());
    for (int i = 0; i < expected.getResultsCount(); i++) {
      ProductSearchResults.Result expectedResult = expected.getResults(i);
      StructuredRecord actualResult = results.get(i);
      assertResultEquals(expectedResult, actualResult);
    }

    List<StructuredRecord> groupedResults = actual.get(ProductSearchResultsSchema.GROUPED_RESULTS_FIELD_NAME);
    Assert.assertNotNull(groupedResults);
    Assert.assertEquals(expected.getProductGroupedResultsCount(), groupedResults.size());
    for (int i = 0; i < expected.getProductGroupedResultsCount(); i++) {
      ProductSearchResults.GroupedResult expectedResult = expected.getProductGroupedResults(i);
      StructuredRecord actualResult = groupedResults.get(i);
      assertGroupedResultEquals(expectedResult, actualResult);
    }
  }

  private void assertGroupedResultEquals(ProductSearchResults.GroupedResult expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    List<StructuredRecord> position = actual.get(ProductSearchResultsSchema.GroupedResult.POSITION_FIELD_NAME);
    assertPositionEqual(expected.getBoundingPoly(), position);

    List<StructuredRecord> results = actual.get(ProductSearchResultsSchema.GroupedResult.RESULTS_FIELD_NAME);
    Assert.assertNotNull(results);
    Assert.assertEquals(expected.getResultsCount(), results.size());
    for (int i = 0; i < expected.getResultsCount(); i++) {
      ProductSearchResults.Result expectedResult = expected.getResults(i);
      StructuredRecord actualResult = results.get(i);
      assertResultEquals(expectedResult, actualResult);
    }
  }

  private void assertResultEquals(ProductSearchResults.Result expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getImage(), actual.get(ProductSearchResultsSchema.Result.IMAGE_FIELD_NAME));
    Assert.assertEquals(
      expected.getScore(),
      actual.<Float>get(ProductSearchResultsSchema.Result.SCORE_FIELD_NAME),
      DELTA);
    assertProductEquals(expected.getProduct(), actual.get(ProductSearchResultsSchema.Result.PRODUCT_FIELD_NAME));
  }

  private void assertProductEquals(Product expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getName(), actual.get(ProductSearchResultsSchema.Product.NAME_FIELD_NAME));
    Assert.assertEquals(expected.getDisplayName(),
      actual.get(ProductSearchResultsSchema.Product.DISPLAY_NAME_FIELD_NAME));
    Assert.assertEquals(expected.getDescription(),
      actual.get(ProductSearchResultsSchema.Product.DESCRIPTION_FIELD_NAME));
    Assert.assertEquals(expected.getProductCategory(),
      actual.get(ProductSearchResultsSchema.Product.PRODUCT_CATEGORY_FIELD_NAME));

    List<StructuredRecord> labels = actual.get(ProductSearchResultsSchema.Product.PRODUCT_LABELS_FIELD_NAME);
    Assert.assertNotNull(labels);
    Assert.assertEquals(expected.getProductLabelsCount(), labels.size());
    for (int i = 0; i < expected.getProductLabelsCount(); i++) {
      Product.KeyValue expectedLabel = expected.getProductLabels(i);
      StructuredRecord actualLabel = labels.get(i);
      assertLabelEquals(expectedLabel, actualLabel);
    }
  }

  private void assertLabelEquals(Product.KeyValue expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getKey(), actual.get(ProductSearchResultsSchema.KeyValue.KEY_FIELD_NAME));
    Assert.assertEquals(expected.getValue(), actual.get(ProductSearchResultsSchema.KeyValue.VALUE_FIELD_NAME));
  }
}
