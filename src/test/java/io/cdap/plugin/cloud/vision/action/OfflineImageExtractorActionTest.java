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

package io.cdap.plugin.cloud.vision.action;

import com.google.auth.Credentials;
import com.google.cloud.storage.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.mock.action.MockActionContext;
import io.cdap.plugin.cloud.vision.CloudVisionConstants;
import io.cdap.plugin.cloud.vision.CredentialsHelper;
import io.cdap.plugin.cloud.vision.transform.ImageFeature;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Test class for {@link OfflineImageExtractorAction}.
 */
public class OfflineImageExtractorActionTest {

  private static final String PROJECT = System.getProperty("project", CloudVisionConstants.AUTO_DETECT);
  private static final String SERVICE_ACCOUNT_FILE_PATH = System.getProperty("serviceFilePath", CloudVisionConstants.AUTO_DETECT);
  private static final String PATH = System.getProperty("path", "gs://cloud-vision-cdap-text-image-extractor-offline");
  private static final String PATH_PATTERN = "%s/%s/";
  private static final String RESULT_PATH_PATTERN = "%s/%s";

  private static final String JPG_CONTENT_TYPE = "image/jpeg";
  private static final String OBJECT_IMAGE_NAME = "multiple_objects.jpg";
  private static final String SAFE_IMAGE_NAME = "safe_search.jpg";
  private static final String CROP_HINTS_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/crop_hints/bubble.jpeg";
  private static final String FACE_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/face/faces.jpeg";
  private static final String PROPERTIES_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/image_properties/bali.jpeg";
  private static final String LABELS_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/label/setagaya.jpeg";
  private static final String LANDMARKS_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/landmark/st_basils.jpeg";
  private static final String LOGOS_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/logo/google_logo.jpg";
  private static final String OBJECTS_IMAGE_FILE_PATH = String.format(RESULT_PATH_PATTERN, PATH, OBJECT_IMAGE_NAME);
  private static final String SAFE_SEARCH_IMAGE_FILE_PATH = String.format(RESULT_PATH_PATTERN, PATH, SAFE_IMAGE_NAME);
  private static final String TEXT_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/ocr/sign.jpg";
  private static final String WEB_DETECTION_IMAGE_FILE_PATH = "gs://cloud-samples-data/vision/web/carnaval.jpeg";
  private static final String RESULT_FILE_NAME = "output-1-to-1.json";

  private static final String BATCH_SIZE = "20";

  private static final OfflineImageExtractorActionConfig CONFIG = new OfflineImageExtractorActionConfig(
          SERVICE_ACCOUNT_FILE_PATH,
          ImageFeature.FACE.getDisplayName(),
          null,
          null,
          true,
          null,
          null,
          BATCH_SIZE
  );

  private static Storage storage;
  private static Bucket bucket;

  private static void initTestsData() throws FileNotFoundException {
    String path = String.format("src/test/resources/%s", OBJECT_IMAGE_NAME);

    FileInputStream serviceAccountStream = new FileInputStream(path);
    bucket.create(OBJECT_IMAGE_NAME, serviceAccountStream, JPG_CONTENT_TYPE, Bucket.BlobWriteOption.doesNotExist());

    path = String.format("src/test/resources/%s", SAFE_IMAGE_NAME);

    serviceAccountStream = new FileInputStream(path);
    bucket.create(SAFE_IMAGE_NAME, serviceAccountStream, JPG_CONTENT_TYPE, Bucket.BlobWriteOption.doesNotExist());
  }

  private static Storage getStorage(String project, @Nullable Credentials credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      builder.setCredentials(credentials);
    }
    return builder.build().getService();
  }

  private static void deleteBucket(Storage storage, Bucket bucket) {
    if (bucket == null || bucket.list() == null) {
      return;
    }

    for (Blob blob : bucket.list().iterateAll()) {
      storage.delete(blob.getBlobId());
    }
    bucket.delete(Bucket.BucketSourceOption.metagenerationMatch());
  }

  @Before
  public void testSetup() throws Exception {
    Credentials credentials = CredentialsHelper.getCredentials(SERVICE_ACCOUNT_FILE_PATH);
    String projectId = CredentialsHelper.getProjectId(PROJECT);
    storage = getStorage(projectId, credentials);

    GCSPath path = GCSPath.from(PATH);
    String bucketName = path.getBucket();
    bucket = storage.get(bucketName);
    if (bucket != null) {
      deleteBucket(storage, bucket);
    }

    BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName)
            .setStorageClass(StorageClass.STANDARD)
            .setLocation("us-central1")
            .build();
    bucket = storage.create(bucketInfo);
    initTestsData();
  }

  @After
  public void destroy() {
    deleteBucket(storage, bucket);
  }

  @Test
  public void testRunCropHintsFeature() throws Exception {
    String folder = "crop_hints";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(CROP_HINTS_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.CROP_HINTS.getDisplayName())
            .setAspectRatios("1.66667")
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, CROP_HINTS_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunFaceFeature() throws Exception {
    String folder = "face";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(FACE_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, FACE_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunImagePropertiesFeature() throws Exception {
    String folder = "properties";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(PROPERTIES_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.IMAGE_PROPERTIES.getDisplayName())
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, PROPERTIES_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunLabelsFeature() throws Exception {
    String folder = "labels";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(LABELS_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.LABELS.getDisplayName())
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, LABELS_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunLandmarksFeature() throws Exception {
    String folder = "landmarks";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(LANDMARKS_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.LANDMARKS.getDisplayName())
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, LANDMARKS_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunLogosFeature() throws Exception {
    String folder = "logos";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(LOGOS_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.LOGOS.getDisplayName())
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, LOGOS_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunObjectLocalizationFeature() throws Exception {
    String folder = "objects";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(OBJECTS_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.OBJECT_LOCALIZATION.getDisplayName())
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, OBJECTS_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunTextFeature() throws Exception {
    String folder = "text";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(TEXT_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.TEXT.getDisplayName())
            .setLanguageHints("en")
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, TEXT_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunSafeSearchFeature() throws Exception {
    String folder = "safe";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(SAFE_SEARCH_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.EXPLICIT_CONTENT.getDisplayName())
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, SAFE_SEARCH_IMAGE_FILE_PATH);
  }

  @Test
  public void testRunWebDetectionFeature() throws Exception {
    String folder = "web";
    String resultFolderPath = String.format(PATH_PATTERN, PATH, folder);
    OfflineImageExtractorActionConfig config = OfflineImageExtractorActionConfig.builder(CONFIG)
            .setSourcePath(WEB_DETECTION_IMAGE_FILE_PATH)
            .setDestinationPath(resultFolderPath)
            .setFeatures(ImageFeature.WEB_DETECTION.getDisplayName())
            .build();

    OfflineImageExtractorAction action = new OfflineImageExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    validateResult(folder, WEB_DETECTION_IMAGE_FILE_PATH);
  }

  private void validateResult(String folder, String expectedUri) throws InterruptedException {
    Thread.sleep(30000);

    Blob blob = bucket.get(String.format(RESULT_PATH_PATTERN, folder, RESULT_FILE_NAME));
    Assert.assertTrue(blob.exists());

    String content = new String(blob.getContent());
    JsonParser parser = new JsonParser();
    JsonElement jsonTree = parser.parse(content);
    String uri = jsonTree.getAsJsonObject()
            .get("responses")
            .getAsJsonArray()
            .get(0)
            .getAsJsonObject()
            .get("context")
            .getAsJsonObject()
            .get("uri")
            .getAsString();

    Assert.assertEquals(expectedUri, uri);
  }
}
