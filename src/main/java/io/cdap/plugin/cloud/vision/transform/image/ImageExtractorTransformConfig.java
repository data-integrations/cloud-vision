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

package io.cdap.plugin.cloud.vision.transform.image;

import com.google.cloud.vision.v1.BoundingPoly;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConstants;
import io.cdap.plugin.cloud.vision.transform.ProductCategory;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Defines a {@link PluginConfig} that Image and Document Extractor transforms can use.
 */
public class ImageExtractorTransformConfig extends ExtractorTransformConfig {

  @Name(ExtractorTransformConstants.PRODUCT_SET)
  @Description("Resource name of a ProductSet to be searched for similar images.")
  @Macro
  @Nullable
  private String productSet;

  @Name(ExtractorTransformConstants.PRODUCT_CATEGORIES)
  @Description("List of product categories to search in.")
  @Macro
  @Nullable
  private String productCategories;

  @Name(ExtractorTransformConstants.BOUNDING_POLYGON)
  @Description("Bounding polygon for the image detection.")
  @Macro
  @Nullable
  private String boundingPolygon;

  @Name(ExtractorTransformConstants.FILTER)
  @Description("Filtering expression to restrict search results based on Product labels.")
  @Macro
  @Nullable
  private String filter;

  public ImageExtractorTransformConfig(String project, String serviceFilePath, String pathField,
                                       String outputField, String features,
                                       @Nullable String languageHints, @Nullable String aspectRatios,
                                       @Nullable Boolean includeGeoResults, @Nullable String schema,
                                       @Nullable String productSet, @Nullable String productCategories,
                                       @Nullable String boundingPolygon, @Nullable String filter) {
    super(project, serviceFilePath, pathField, outputField, features,
            languageHints, aspectRatios, includeGeoResults, schema);
    this.productSet = productSet;
    this.productCategories = productCategories;
    this.boundingPolygon = boundingPolygon;
    this.filter = filter;
  }

  @Nullable
  public String getProductSet() {
    return productSet;
  }

  @Nullable
  public String getProductCategories() {
    return productCategories;
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  public ProductCategory getProductCategory() {
    return Objects.requireNonNull(ProductCategory.fromDisplayName(productCategories));
  }

  @Nullable
  public String getBoundingPolygon() {
    return boundingPolygon;
  }

  @Nullable
  public BoundingPoly getBoundingPoly() {
    if (Strings.isNullOrEmpty(boundingPolygon)) {
      return null;
    }

    BoundingPoly.Builder builder = BoundingPoly.newBuilder();
    try {
      JsonFormat.parser().ignoringUnknownFields().merge(boundingPolygon, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      String errorMessage = String.format("Could not parse bounding polygon string: '%s'", boundingPolygon);
      throw new IllegalStateException(errorMessage, e);
    }
  }

  /**
   * Validates {@link ImageExtractorTransformConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(ExtractorTransformConstants.PATH_FIELD) && Strings.isNullOrEmpty(getPathField())) {
      collector.addFailure("Path field must be specified", null)
              .withConfigProperty(ExtractorTransformConstants.PATH_FIELD);
    }
    if (!containsMacro(ExtractorTransformConstants.BOUNDING_POLYGON) && !Strings.isNullOrEmpty(boundingPolygon)) {
      try {
        getBoundingPoly();
      } catch (IllegalStateException e) {
        collector.addFailure("Could not parse bounding polygon string.", null)
                .withConfigProperty(ExtractorTransformConstants.BOUNDING_POLYGON);
      }
    }
  }
}
