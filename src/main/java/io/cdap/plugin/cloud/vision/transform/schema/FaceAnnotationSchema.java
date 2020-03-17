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

package io.cdap.plugin.cloud.vision.transform.schema;

import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.Likelihood;
import io.cdap.cdap.api.data.schema.Schema;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link com.google.cloud.vision.v1.FaceAnnotation} mapped to a record with following fields.
 */
public class FaceAnnotationSchema {

  /**
   * Roll angle, which indicates the amount of clockwise/anti-clockwise rotation of the face relative to the image
   * vertical about the axis perpendicular to the face. Range [-180,180].
   */
  public static final String ROLL_ANGLE_FIELD_NAME = "rollAngle";
  /**
   * Yaw angle, which indicates the leftward/rightward angle that the face is pointing relative to the vertical plane
   * perpendicular to the image. Range [-180,180].
   */
  public static final String PAN_ANGLE_FIELD_NAME = "panAngle";
  /**
   * Pitch angle, which indicates the upwards/downwards angle that the face is pointing relative to the image's
   * horizontal plane. Range [-180,180].
   */
  public static final String TILT_ANGLE_FIELD_NAME = "tiltAngle";
  /**
   * Detection confidence. Range [0, 1].
   */
  public static final String DETECTION_CONFIDENCE_FIELD_NAME = "detectionConfidence";
  /**
   * Face landmarking confidence. Range [0, 1].
   */
  public static final String LANDMARKING_CONFIDENCE_FIELD_NAME = "landmarkingConfidence";
  /**
   * Anger likelihood. Possible values are defined by {@link Likelihood}.
   */
  public static final String ANGER_FIELD_NAME = "angerLikelihood";
  /**
   * Joy likelihood. Possible values are defined by {@link Likelihood}.
   */
  public static final String JOY_FIELD_NAME = "joyLikelihood";
  /**
   * Surprise likelihood. Possible values are defined by {@link Likelihood}.
   */
  public static final String SURPRISE_FIELD_NAME = "surpriseLikelihood";
  /**
   * Blurred likelihood. Possible values are defined by {@link Likelihood}.
   */
  public static final String BLURRED_FIELD_NAME = "blurredLikelihood";
  /**
   * Under exposed likelihood. Possible values are defined by {@link Likelihood}.
   */
  public static final String UNDER_EXPOSED_FIELD_NAME = "underExposedLikelihood";
  /**
   * Sorrow likelihood. Possible values are defined by {@link Likelihood}.
   */
  public static final String SORROW_FIELD_NAME = "sorrowLikelihood";
  /**
   * Headwear likelihood. Possible values are defined by {@link Likelihood}.
   */
  public static final String HEADWEAR_FIELD_NAME = "headwearLikelihood";
  /**
   * The bounding polygon around the face. The bounding box is computed to "frame" the face in
   * accordance with human expectations. It is based on the landmarker results. Note that one or
   * more x and/or y coordinates may not be generated if only a partial face appears in the image
   * to be annotated.
   */
  public static final String BOUNDING_POLY_NAME = "boundingPoly";
  /**
   * The bounding polygon, tighter than the {@link FaceAnnotationSchema#BOUNDING_POLY_NAME},
   * encloses only the skin part of the face. Typically, it is used to eliminate the face from any
   * image analysis that detects the "amount of skin" visible in an image. It is not based on the
   * landmarker results, only on the initial face detection, hence the <code>fd</code>
   * (face detection) prefix.
   */
  public static final String FD_BOUNDING_POLY_NAME = "fdBoundingPoly";
  /**
   * Detected face landmarks.
   */
  public static final String LANDMARKS_FIELD_NAME = "landmarks";
  public static final Schema SCHEMA = Schema.recordOf(
          "face-annotation-component-record",
          Schema.Field.of(ROLL_ANGLE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(PAN_ANGLE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(TILT_ANGLE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(DETECTION_CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(LANDMARKING_CONFIDENCE_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
          Schema.Field.of(ANGER_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(JOY_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(SURPRISE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(BLURRED_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(UNDER_EXPOSED_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(SORROW_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(HEADWEAR_FIELD_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(BOUNDING_POLY_NAME, Schema.arrayOf(VertexSchema.SCHEMA)),
          Schema.Field.of(FD_BOUNDING_POLY_NAME, Schema.arrayOf(VertexSchema.SCHEMA2)),
          Schema.Field.of(LANDMARKS_FIELD_NAME, Schema.arrayOf(FaceLandmark.SCHEMA))
  );

  private FaceAnnotationSchema() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * {@link com.google.cloud.vision.v1.FaceAnnotation.Landmark} mapped to a record with the
   * following fields.
   */
  public static class FaceLandmark {

    /**
     * Face landmark type.
     */
    public static final String TYPE_FIELD_NAME = "type";

    /**
     * X coordinate.
     */
    public static final String X_FIELD_NAME = "x";

    /**
     * Y coordinate.
     */
    public static final String Y_FIELD_NAME = "y";

    /**
     * Z coordinate.
     */
    public static final String Z_FIELD_NAME = "z";

    public static final Schema SCHEMA = Schema.recordOf("face-landmark-record",
            // The following field is not a simple type as it is defined as an Enum
            Schema.Field.of(TYPE_FIELD_NAME, Schema.enumWith(
                    FaceAnnotationSchema.getIterableFromEnum(
                            FaceAnnotation.Landmark.Type.values()))),
            Schema.Field.of(X_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(Y_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(Z_FIELD_NAME, Schema.of(Schema.Type.FLOAT)));

    private FaceLandmark() {
      throw new AssertionError("Should not instantiate static utility class.");
    }
  }

  /**
   * {@link com.google.cloud.vision.v1.FaceAnnotation.Landmark} mapped to a record with the
   * following fields.
   */
  public static class FaceLandmark2 {

    /**
     * Face landmark type.
     */
    public static final String TYPE_FIELD_NAME = "type";

    /**
     * X coordinate.
     */
    public static final String X_FIELD_NAME = "x";

    /**
     * Y coordinate.
     */
    public static final String Y_FIELD_NAME = "y";

    /**
     * Z coordinate.
     */
    public static final String Z_FIELD_NAME = "z";

    public static final Schema SCHEMA = Schema.recordOf("face-landmark-record2",
            // The following field is not a simple type as it is defined as an Enum
            Schema.Field.of(TYPE_FIELD_NAME, Schema.enumWith(
                    FaceAnnotationSchema.getIterableFromEnum(
                            FaceAnnotation.Landmark.Type.values()))),
            Schema.Field.of(X_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(Y_FIELD_NAME, Schema.of(Schema.Type.FLOAT)),
            Schema.Field.of(Z_FIELD_NAME, Schema.of(Schema.Type.FLOAT)));

    private FaceLandmark2() {
      throw new AssertionError("Should not instantiate static utility class.");
    }
  }

  protected static <T> List<String> getIterableFromEnum(T[] input) {
    ArrayList<String> list = new ArrayList<>(input.length);
    for (T value : input) {
      list.add(value.toString());
    }
    return list;
  }
}
