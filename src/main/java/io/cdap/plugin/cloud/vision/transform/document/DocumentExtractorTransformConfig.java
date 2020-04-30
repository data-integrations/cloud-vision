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

package io.cdap.plugin.cloud.vision.transform.document;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Defines a {@link PluginConfig} that Document Extractor transform can use.
 */
public class DocumentExtractorTransformConfig extends ExtractorTransformConfig {

  @Name(DocumentExtractorTransformConstants.CONTENT_FIELD)
  @Description("Field in the input schema containing the file content, represented as a stream of bytes.")
  @Macro
  @Nullable
  private String contentField;

  @Name(DocumentExtractorTransformConstants.MIME_TYPE)
  @Description("The type of the file. Currently only 'application/pdf', 'image/tiff' and 'image/gif' are supported. " +
    "Wildcards are not supported.")
  @Macro
  private String mimeType;

  @Name(DocumentExtractorTransformConstants.PAGES)
  @Description("Pages in the file to perform image annotation.")
  @Macro
  private String pages;

  public DocumentExtractorTransformConfig(String project, String serviceFilePath, String pathField, String outputField,
                                          String features, @Nullable String languageHints,
                                          @Nullable String aspectRatios, @Nullable Boolean includeGeoResults,
                                          @Nullable String schema, @Nullable String contentField, String mimeType,
                                          String pages) {
    super(project, serviceFilePath, pathField, outputField, features, languageHints, aspectRatios, includeGeoResults,
      schema);
    this.contentField = contentField;
    this.mimeType = mimeType;
    this.pages = pages;
  }

  @Nullable
  public String getContentField() {
    return contentField;
  }

  public String getMimeType() {
    return mimeType;
  }

  public String getPages() {
    return pages;
  }

  /**
   * Convenience method that splits a comma separated string of values and turn those into a List<Integer>.
   *
   * @return {@link List<Integer>}
   */
  public List<Integer> getPagesList() {
    if (Strings.isNullOrEmpty(pages)) {
      return Collections.emptyList();
    }

    return Arrays.stream(pages.split(","))
      .map(Integer::valueOf)
      .collect(Collectors.toList());
  }

  /**
   * Validates {@link DocumentExtractorTransformConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(ExtractorTransformConstants.PATH_FIELD) &&
      !containsMacro(DocumentExtractorTransformConstants.CONTENT_FIELD) &&
      (Strings.isNullOrEmpty(getPathField()) && Strings.isNullOrEmpty(getContentField()) ||
        !Strings.isNullOrEmpty(getPathField()) && !Strings.isNullOrEmpty(getContentField()))) {
      collector.addFailure("Either path field or content field must be specified", null)
        .withConfigProperty(ExtractorTransformConstants.PATH_FIELD)
        .withConfigProperty(DocumentExtractorTransformConstants.CONTENT_FIELD);
    }
    if (!containsMacro(DocumentExtractorTransformConstants.MIME_TYPE) && Strings.isNullOrEmpty(getMimeType())) {
      collector.addFailure("Mime type must be specified", null)
        .withConfigProperty(DocumentExtractorTransformConstants.MIME_TYPE);
    }
  }

  /**
   * Validates input schema and checks for type compatibility.
   *
   * @param inputSchema input schema.
   * @param collector   failure collector.
   */
  public void validateInputSchema(Schema inputSchema, FailureCollector collector) {
    Schema.Field contentField = inputSchema.getField(getContentField());
    Schema.Field pathField = inputSchema.getField(getPathField());

    if (pathField != null && pathField.getSchema().getType() != Schema.Type.STRING) {
      collector.addFailure(String.format("Path field '%s' is expected to be a string", getPathField()), null)
        .withInputSchemaField(getPathField());
      return;
    }

    if (contentField != null && contentField.getSchema().getType() != Schema.Type.STRING) {
      collector.addFailure(String.format("Content field '%s' is expected to be a string", getContentField()), null)
        .withInputSchemaField(getContentField());
    }
  }

  /**
   * Validates specified schema and checks for required fields.
   *
   * @param providedSchema user-provided schema.
   * @param collector      failure collector.
   */
  public void validateOutputSchema(Schema providedSchema, FailureCollector collector) {
    Schema.Field outputField = providedSchema.getField(getOutputField());
    if (outputField == null) {
      collector.addFailure(String.format("Schema must contain '%s' output field", getOutputField()), null)
        .withConfigProperty(ExtractorTransformConstants.SCHEMA);
    } else {
      Schema pagesSchema = outputField.getSchema();
      if (pagesSchema.getType() != Schema.Type.ARRAY) {
        collector.addFailure(String.format("Output field '%s' is expected to be an array", getOutputField()), null)
          .withOutputSchemaField(getOutputField());
      } else {
        Schema pageSchema = pagesSchema.getComponentSchema();
        if (pageSchema.getField(DocumentExtractorTransformConstants.FEATURE_FIELD_NAME) == null) {
          String errorMessage = String.format("Schema of the output field '%s' must contain '%s' feature field",
            getOutputField(), DocumentExtractorTransformConstants.FEATURE_FIELD_NAME);
          collector.addFailure(errorMessage, null).withOutputSchemaField(getOutputField());
        }
      }
    }
  }
}
