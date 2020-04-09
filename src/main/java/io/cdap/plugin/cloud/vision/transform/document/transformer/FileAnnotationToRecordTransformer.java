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

package io.cdap.plugin.cloud.vision.transform.document.transformer;

import com.google.cloud.vision.v1.AnnotateFileResponse;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.document.DocumentExtractorTransformConfig;
import io.cdap.plugin.cloud.vision.transform.document.DocumentExtractorTransformConstants;
import io.cdap.plugin.cloud.vision.transform.transformer.ImageAnnotationToRecordTransformer;
import io.cdap.plugin.cloud.vision.transform.transformer.TransformerFactory;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms {@link AnnotateFileResponse} to {@link StructuredRecord} according to the specified schema.
 */
public class FileAnnotationToRecordTransformer {

  private static final Schema SINGLE_FIELD_RECORD_SCHEMA = Schema.recordOf("single-field",
    Schema.Field.of("dummy", Schema.of(Schema.Type.STRING)));
  private static final StructuredRecord SINGLE_FIELD_RECORD = StructuredRecord.builder(SINGLE_FIELD_RECORD_SCHEMA)
    .set("dummy", "dummy")
    .build();

  private final ImageAnnotationToRecordTransformer transformer;
  private final Schema schema;
  private final String outputFieldName;

  public FileAnnotationToRecordTransformer(DocumentExtractorTransformConfig config, Schema schema) {
    this.schema = schema;
    this.outputFieldName = config.getOutputField();
    // reuse ImageAnnotationToRecordTransformer to perform mappings
    Schema featureSchema = getImageFeatureSchema(outputFieldName, schema);
    Schema.Field featureField = Schema.Field.of(DocumentExtractorTransformConstants.FEATURE_FIELD_NAME, featureSchema);
    Schema featureRecordSchema = Schema.recordOf("feature-record", featureField);
    this.transformer = TransformerFactory.createInstance(config.getImageFeature(), featureField.getName(),
      featureRecordSchema);
  }

  /**
   * Retrieves image feature schema. Schema retrieved instead of using constant schema since users are free to
   * choose to not include some of the fields. File Annotation mapped to record with field
   * {@value DocumentExtractorTransformConstants#PAGE_FIELD_NAME} for page number and
   * {@value DocumentExtractorTransformConstants#FEATURE_FIELD_NAME} field for extracted image feature.
   *
   * @param outputFieldName specified output field name for extracted image feature.
   * @param schema          plugin output schema.
   * @return image feature schema.
   */
  private static Schema getImageFeatureSchema(String outputFieldName, Schema schema) {
    Schema pageSchema = getPageSchema(outputFieldName, schema);
    Schema.Field field = pageSchema.getField(DocumentExtractorTransformConstants.FEATURE_FIELD_NAME);

    return field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
  }

  /**
   * Retrieves File Annotation page schema. Schema retrieved instead of using constant schema since users are free to
   * choose to not include some of the fields. File Annotation mapped to record with field
   * {@value DocumentExtractorTransformConstants#PAGE_FIELD_NAME} for page number and
   * {@value DocumentExtractorTransformConstants#FEATURE_FIELD_NAME} field for extracted image feature.
   *
   * @param outputFieldName specified output field name for extracted image feature.
   * @param schema          plugin output schema.
   * @return File Annotation page schema.
   */
  private static Schema getPageSchema(String outputFieldName, Schema schema) {
    Schema.Field field = schema.getField(outputFieldName);
    Schema pagesSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    return pagesSchema.getComponentSchema().isNullable() ? pagesSchema.getComponentSchema().getNonNullable()
      : pagesSchema.getComponentSchema();
  }

  /**
   * Transforms input record {@link StructuredRecord} and {@link AnnotateFileResponse} to {@link StructuredRecord}
   * including extracted file features to the result record according to the specified output field name and schema.
   * For example, given input record:
   * <pre>
   *   document: "gs://some-bucket/logos.pdf"
   *   description: "Google and CDAP logos"
   * </pre>
   * with "document" output field will be transformed to the following output record(actual result depends on the
   * feature type):
   * <pre>
   *   document: [
   *     page: 1
   *     feature:
   *       mid: /g/120yr454",
   *       description: "Google",
   *       ...,
   *     page: 2
   *     feature:
   *       mid: "/m/045c7b",
   *       description: "CDAP",
   *       ...,
   *   ]
   *   description: "Google and CDAP logos"
   * </pre>
   * If the specified output field name already exists in the input record, it will be overwritten.
   *
   * @param input                input record to transform.
   * @param annotateFileResponse annotate file response which contains extracted features.
   * @return transformed {@link StructuredRecord} which contains extracted features.
   */
  public StructuredRecord transform(StructuredRecord input, AnnotateFileResponse annotateFileResponse) {
    List<StructuredRecord> pages = annotateFileResponse.getResponsesList().stream()
      .map(this::annotateImageResponseToPageRecord)
      .collect(Collectors.toList());

    return getOutputRecordBuilder(input)
      .set(outputFieldName, pages)
      .build();
  }

  private StructuredRecord annotateImageResponseToPageRecord(AnnotateImageResponse annotateImageResponse) {
    Schema pageSchema = getPageSchema(outputFieldName, schema);
    StructuredRecord.Builder builder = StructuredRecord.builder(pageSchema);
    if (pageSchema.getField(DocumentExtractorTransformConstants.PAGE_FIELD_NAME) != null) {
      int pageNumber = annotateImageResponse.getContext().getPageNumber();
      builder.set(DocumentExtractorTransformConstants.PAGE_FIELD_NAME, pageNumber);
    }

    StructuredRecord transformed = transformer.transform(SINGLE_FIELD_RECORD, annotateImageResponse);
    builder.set(DocumentExtractorTransformConstants.FEATURE_FIELD_NAME,
      transformed.get(DocumentExtractorTransformConstants.FEATURE_FIELD_NAME));

    return builder.build();
  }

  /**
   * Creates output record builder and copies corresponding input record field values.
   *
   * @param input input record.
   * @return output record builder with copied input record field values.
   */
  private StructuredRecord.Builder getOutputRecordBuilder(StructuredRecord input) {
    Schema inputRecordSchema = input.getSchema();
    StructuredRecord.Builder outputRecordBuilder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      if (inputRecordSchema.getField(field.getName()) == null) {
        continue;
      }
      // copy input record field values
      outputRecordBuilder.set(field.getName(), input.get(field.getName()));
    }

    return outputRecordBuilder;
  }
}
