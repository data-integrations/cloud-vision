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

import com.google.cloud.vision.v1.BoundingPoly;
import com.google.cloud.vision.v1.Vertex;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.VertexSchema;
import org.junit.Assert;
import java.util.Arrays;
import java.util.List;

/**
 * Base annotations to record transformer test.
 */
public class BaseAnnotationsToRecordTransformerTest {

  protected static final Schema INPUT_RECORD_SCHEMA = Schema.recordOf(
    "input-record-schema",
    Schema.Field.of("path", Schema.of(Schema.Type.STRING)));

  protected static final StructuredRecord INPUT_RECORD = StructuredRecord.builder(INPUT_RECORD_SCHEMA)
    .set("path", "gs://dummy/image.png")
    .build();

  protected static final BoundingPoly POSITION = BoundingPoly.newBuilder()
    .addAllVertices(Arrays.asList(
      Vertex.newBuilder().setX(0).setY(0).build(),
      Vertex.newBuilder().setX(100).setY(0).build(),
      Vertex.newBuilder().setX(100).setY(100).build(),
      Vertex.newBuilder().setX(0).setY(100).build()
    ))
    .build();

  protected static final int SINGLE_FEATURE_INDEX = 0;
  protected static final double DELTA = 0.0001;

  protected void assertPositionEqual(BoundingPoly expected, List<StructuredRecord> actual) {
    Assert.assertNotNull(actual);
    for (int i = 0; i < expected.getVerticesList().size(); i++) {
      Vertex expectedVertex = expected.getVertices(i);
      StructuredRecord actualVertex = actual.get(i);
      Assert.assertNotNull(actualVertex);

      Assert.assertEquals(expectedVertex.getX(), (int) actualVertex.get(VertexSchema.X_FIELD_NAME));
      Assert.assertEquals(expectedVertex.getY(), (int) actualVertex.get(VertexSchema.Y_FIELD_NAME));
    }
  }
}
