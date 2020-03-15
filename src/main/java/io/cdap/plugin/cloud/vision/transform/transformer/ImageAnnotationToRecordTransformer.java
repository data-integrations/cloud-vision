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
import com.google.cloud.vision.v1.Vertex;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.VertexSchema;

import java.util.Objects;


/**
 * Transforms {@link AnnotateImageResponse} to {@link StructuredRecord} according to the specified schema.
 */
public abstract class ImageAnnotationToRecordTransformer {

  protected final Schema schema;
  protected final String outputFieldName;

  public ImageAnnotationToRecordTransformer(Schema schema, String outputFieldName) {
    this.schema = schema;
    this.outputFieldName = outputFieldName;
  }

  /**
   * Transforms input record {@link StructuredRecord} and {@link AnnotateImageResponse} to {@link StructuredRecord}
   * including extracted image features to the result record according to the specified output field name and schema.
   * For example, given input record:
   * <pre>
   *   image: "gs://some-bucket/child.jpg"
   *   description: "Child's face"
   * </pre>
   * with "image" output field will be transformed to the output record like(actual result depends on the feature type):
   * <pre>
   *   image: [
   *     anger: VERY_UNLIKELY,
   *     joy: VERY_LIKELY,
   *     surprise: VERY_UNLIKELY,
   *     ...,
   *   ]
   *   description: "Child's face"
   * </pre>
   * If the specified output field name already exists in the input record, it will be overwritten.
   *
   * @param input                 input record to transform.
   * @param annotateImageResponse annotate image response which contains extracted features.
   * @return transformed {@link StructuredRecord} which contains extracted features.
   */
  public abstract StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse);

  /**
   * Creates output record builder and copies corresponding input record field values.
   *
   * @param input input record.
   * @return output record builder with copied input record field values.
   */
  public StructuredRecord.Builder getOutputRecordBuilder(StructuredRecord input) {
    Schema inputRecordSchema = input.getSchema();
    // schema is the output schema
    StructuredRecord.Builder outputRecordBuilder = StructuredRecord.builder(schema);
    // Loop through the input fields and copy them to the output
    for (Schema.Field inputField : inputRecordSchema.getFields()) {
      if (schema.getField(inputField.getName()) == null) {
        continue; // Not found at the output
      }

      // copy input record field values
      outputRecordBuilder.set(inputField.getName(), input.get(inputField.getName()));
    }

    return outputRecordBuilder;
  }

  protected StructuredRecord extractVertex(Vertex vertex, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(VertexSchema.X_FIELD_NAME) != null) {
      builder.set(VertexSchema.X_FIELD_NAME, vertex.getX());
    }
    if (schema.getField(VertexSchema.Y_FIELD_NAME) != null) {
      builder.set(VertexSchema.Y_FIELD_NAME, vertex.getY());
    }

    return builder.build();
  }

  /**
   * Extracts non-nullable component's schema of the specified array field.
   *
   * @param field array field.
   * @return non-nullable component's schema of the specified array field
   */
  protected Schema getComponentSchema(Schema.Field field) {
    Schema arraySchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema componentSchema = Objects.requireNonNull(arraySchema.getComponentSchema());
    return componentSchema.isNullable() ? componentSchema.getNonNullable() : componentSchema;
  }
}
