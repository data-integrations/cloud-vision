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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Google Cloud Vision Image Extractor Transform which can be used in conjunction with the file path
 * batch source to extract enrichments from each image based on selected features.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(ImageExtractorTransform.PLUGIN_NAME)
@Description("Extracts enrichments from each image based on selected features.")
public class ImageExtractorTransform extends Transform<StructuredRecord, StructuredRecord> {

  /**
   * Image Extractor Transform plugin name.
   */
  public static final String PLUGIN_NAME = "ImageExtractor";

  private ImageAnnotatorClient imageAnnotatorClient;
  private ImageAnnotationToRecordTransformer transformer;
  private ImageExtractorTransformConfig config;
  private Schema inputSchema;
  private Schema outputSchema;
  private static Logger LOG = LoggerFactory.getLogger(ImageExtractorTransform.class);

  public ImageExtractorTransform(ImageExtractorTransformConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    inputSchema = stageConfigurer.getInputSchema();
    outputSchema = getOutputSchema(inputSchema);
    stageConfigurer.setOutputSchema(outputSchema);
    stageConfigurer.setErrorSchema(ExtractorTransformConfig.ERROR_SCHEMA);
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
    outputSchema = getOutputSchema(context.getInputSchema());
    transformer = TransformerFactory.createInstance(config.getImageFeature(),
            config.getOutputField(), outputSchema);
    imageAnnotatorClient = new ImageAnnotatorClient(config);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    String imagePath = input.get(config.getPathField());
    LOG.info("Processing: " + imagePath);
    try {
      AnnotateImageResponse response = imageAnnotatorClient.extractImageFeature(imagePath);
      StructuredRecord transformed = transformer.transform(input, response);
      emitter.emit(transformed);
    } catch (Exception e) {
      StructuredRecord errorRecord = StructuredRecord.builder(ExtractorTransformConfig.ERROR_SCHEMA)
              .set("error", e.getMessage())
              .build();
      emitter.emitError(new InvalidEntry<>(400, e.getMessage(), errorRecord));
    }
  }

  private Schema getOutputSchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    if (inputSchema.getFields() != null) {
      fields.addAll(inputSchema.getFields());
    }

    fields.add(Schema.Field.of(config.getOutputField(), config.getImageFeature().getSchema()));
    return Schema.recordOf("record", fields);
  }
}
