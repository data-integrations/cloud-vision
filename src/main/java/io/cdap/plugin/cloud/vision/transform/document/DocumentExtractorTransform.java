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

import com.google.cloud.vision.v1.AnnotateImageResponse;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.cloud.vision.transform.ExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.transformer.ImageAnnotationToRecordTransformer;
import io.cdap.plugin.cloud.vision.transform.transformer.TransformerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * This transform plugin can detect and transcribe text from small(up to 5 pages) PDF and TIFF files stored in
 * Cloud Storage.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(DocumentExtractorTransform.PLUGIN_NAME)
@Description("Extracts enrichments from each PDF, TIFF, or GIF document based on selected features.")
public class DocumentExtractorTransform extends Transform<StructuredRecord, StructuredRecord> {

  /**
   * Document Text Extractor Transform plugin name.
   */
  public static final String PLUGIN_NAME = "DocumentExtractor";

  private DocumentAnnotatorClient documentAnnotatorClient;
  private ImageAnnotationToRecordTransformer transformer;
  private DocumentExtractorTransformConfig config;
  private Schema inputSchema;

  public DocumentExtractorTransform(DocumentExtractorTransformConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    inputSchema = configurer.getStageConfigurer().getInputSchema();
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    Schema schema = getSchema();
    Schema configuredSchema = config.getParsedSchema();
    if (configuredSchema == null) {
      configurer.getStageConfigurer().setOutputSchema(schema);
      return;
    }
    ExtractorTransformConfig.validateFieldsMatch(schema, configuredSchema, collector);
    collector.getOrThrowException();
    configurer.getStageConfigurer().setOutputSchema(configuredSchema);
    configurer.getStageConfigurer().setErrorSchema(ExtractorTransformConfig.ERROR_SCHEMA);
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    Schema schema = context.getOutputSchema();
    transformer = TransformerFactory.createInstance(config, schema);
    documentAnnotatorClient = new DocumentAnnotatorClient(config);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    String imagePath = input.get(config.getPathField());
    try {
      AnnotateImageResponse response = documentAnnotatorClient.extractDocumentFeature(imagePath);
      StructuredRecord transformed = transformer.transform(input, response);
      emitter.emit(transformed);
    } catch (Exception e) {
      StructuredRecord errorRecord = StructuredRecord.builder(ExtractorTransformConfig.ERROR_SCHEMA)
        .set("error", e.getMessage())
        .build();
      emitter.emitError(new InvalidEntry<>(400, e.getMessage(), errorRecord));
    }
  }

  public Schema getSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    if (inputSchema.getFields() != null) {
      fields.addAll(inputSchema.getFields());
    }

    fields.add(Schema.Field.of(config.getOutputField(), config.getImageFeature().getSchema()));
    return Schema.recordOf("record", fields);
  }
}
