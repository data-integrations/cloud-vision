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

package io.cdap.plugin.cloud.vision.transform;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.cloud.vision.CloudVisionConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Defines a {@link PluginConfig} that Image and Document Extractor transforms can use.
 */
public class ExtractorTransformConfig extends CloudVisionConfig {

  public static final Schema ERROR_SCHEMA = Schema.recordOf("error",
    Schema.Field.of("error", Schema.of(Schema.Type.STRING)));

  @Name(ExtractorTransformConstants.PATH_FIELD)
  @Description("Field in the input schema containing the path to the image.")
  @Macro
  private String pathField;

  @Name(ExtractorTransformConstants.OUTPUT_FIELD)
  @Description("Field to store the extracted image features. If the specified output field name already exists " +
    "in the input record, it will be overwritten.")
  @Macro
  private String outputField;

  @Name(ExtractorTransformConstants.FEATURES)
  @Description("Features to extract from images.")
  @Macro
  private String features;

  @Name(ExtractorTransformConstants.LANGUAGE_HINTS)
  @Description("Hints to detect the language of the text in the images.")
  @Macro
  @Nullable
  private String languageHints;

  @Name(ExtractorTransformConstants.ASPECT_RATIOS)
  @Description("Ratio of the width to the height of the image. If not specified, the best possible crop is returned.")
  @Macro
  @Nullable
  private String aspectRatios;

  @Name(ExtractorTransformConstants.INCLUDE_GEO_RESULTS)
  @Description("Whether to include results derived from the geo information in the image.")
  @Macro
  @Nullable
  private Boolean includeGeoResults;

  @Name(ExtractorTransformConstants.SCHEMA)
  @Description("Schema of records output by the transform.")
  @Nullable
  private String schema;

  public ExtractorTransformConfig(String project, String serviceFilePath, String pathField, String outputField,
                                  String features, @Nullable String languageHints, @Nullable String aspectRatios,
                                  @Nullable Boolean includeGeoResults, @Nullable String schema) {
    super(project, serviceFilePath);
    this.pathField = pathField;
    this.outputField = outputField;
    this.features = features;
    this.languageHints = languageHints;
    this.aspectRatios = aspectRatios;
    this.includeGeoResults = includeGeoResults;
    this.schema = schema;
  }

  public String getPathField() {
    return pathField;
  }

  public String getOutputField() {
    return outputField;
  }

  public String getFeatures() {
    return features;
  }

  @Nullable
  public String getLanguageHints() {
    return languageHints;
  }

  public List<String> getLanguages() {
    return Strings.isNullOrEmpty(languageHints) ? Collections.emptyList() : Arrays.asList(languageHints.split(","));
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  public ImageFeature getImageFeature() {
    return Objects.requireNonNull(ImageFeature.fromDisplayName(features));
  }

  @Nullable
  public String getAspectRatios() {
    return aspectRatios;
  }

  public List<Float> getAspectRatiosList() {
    if (Strings.isNullOrEmpty(aspectRatios)) {
      return Collections.emptyList();
    }

    return Arrays.stream(aspectRatios.split(","))
      .map(Float::valueOf)
      .collect(Collectors.toList());
  }

  @Nullable
  public Boolean isIncludeGeoResults() {
    return includeGeoResults;
  }

  /**
   * Parses the json representation into a schema object.
   *
   * @return parsed schema object of json representation.
   * @throws RuntimeException if there was an exception parsing the schema.
   */
  @Nullable
  public Schema getParsedSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      // this should not happen, since schema string comes from UI
      throw new IllegalStateException(String.format("Could not parse schema string: '%s'", schema), e);
    }
  }

  /**
   * Validates {@link ExtractorTransformConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    if (!containsMacro(ExtractorTransformConstants.OUTPUT_FIELD) && Strings.isNullOrEmpty(outputField)) {
      collector.addFailure("Output field must be specified", null)
        .withConfigProperty(ExtractorTransformConstants.OUTPUT_FIELD);
    }
    if (!containsMacro(ExtractorTransformConstants.FEATURES)) {
      if (Strings.isNullOrEmpty(features)) {
        collector.addFailure("Features must be specified", null)
          .withConfigProperty(ExtractorTransformConstants.FEATURES);
      } else if (ImageFeature.fromDisplayName(features) == null) {
        collector.addFailure("Invalid image feature name", null)
          .withConfigProperty(ExtractorTransformConstants.FEATURES);
      }
    }
  }

  /**
   * Validate that the provided schema is compatible with the inferred schema. The provided schema is compatible if
   * every field is compatible with the corresponding field in the inferred schema. A field is compatible if it is of
   * the same type or is a nullable version of that type. It is assumed that both schemas are record schemas.
   *
   * @param inferredSchema the inferred schema
   * @param providedSchema the provided schema to check compatibility
   * @param collector      failure collector
   * @throws IllegalArgumentException if the schemas are not type compatible
   */
  public static void validateFieldsMatch(Schema inferredSchema, Schema providedSchema, FailureCollector collector) {
    for (Schema.Field field : providedSchema.getFields()) {
      Schema.Field inferredField = inferredSchema.getField(field.getName());
      Schema inferredFieldSchema = inferredField.getSchema();
      Schema providedFieldSchema = field.getSchema();

      boolean isInferredFieldNullable = inferredFieldSchema.isNullable();
      boolean isProvidedFieldNullable = providedFieldSchema.isNullable();

      Schema inferredFieldNonNullableSchema = isInferredFieldNullable
        ? inferredFieldSchema.getNonNullable() : inferredFieldSchema;
      Schema providedFieldNonNullableSchema = isProvidedFieldNullable ?
        providedFieldSchema.getNonNullable() : providedFieldSchema;

      Schema.Type inferredType = inferredFieldNonNullableSchema.getType();
      Schema.LogicalType inferredLogicalType = inferredFieldNonNullableSchema.getLogicalType();
      Schema.Type providedType = providedFieldNonNullableSchema.getType();
      Schema.LogicalType providedLogicalType = providedFieldNonNullableSchema.getLogicalType();
      if (inferredType != providedType && inferredLogicalType != providedLogicalType) {
        String errorMessage = String.format("Expected field '%s' to be of type '%s', but it is of type '%s'",
          field.getName(), inferredFieldNonNullableSchema.getDisplayName(),
          providedFieldNonNullableSchema.getDisplayName());

        collector.addFailure(errorMessage, String.format("Change field '%s' to be a supported type", field.getName()))
          .withOutputSchemaField(field.getName(), null);
      }

      if (!isInferredFieldNullable && isProvidedFieldNullable) {
        String errorMessage = String.format("Field '%s' should not be nullable", field.getName());
        collector.addFailure(errorMessage, String.format("Change field '%s' to be non-nullable", field.getName()))
          .withOutputSchemaField(field.getName(), null);
      }
    }
  }
}
