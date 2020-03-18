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
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import javax.annotation.Nullable;

/**
 * Test class for {@link OfflineImageExtractorAction}.
 */
public class OfflineImageExtractorActionTest {

  private static final String PROJECT = System.getProperty("project", CloudVisionConstants.AUTO_DETECT);

  private static final String SERVICE_ACCOUNT_FILE_PATH = System.getProperty("serviceFilePath",
          CloudVisionConstants.AUTO_DETECT);
  private static final String BUCKET_NAME = System.getProperty("path", "gs://cloud-vision-cdap-tests");
  private static final String IMAGE_PATH_IN_BUCKET = "images";

  private static final String PATH_PATTERN = "%s/%s/";
  private static final String RESULT_PATH_PATTERN = "%s/%s";

  private static final String JPG_CONTENT_TYPE = "image/jpeg";

  private static final String LOCAL_IMAGE_FOLDER_PATH = "examples/images/jpg";

  private static final String CROP_HINTS_IMAGE_NAME = "bubble.jpg";
  private static final String FACE_IMAGE_NAME = "faces.jpg";
  private static final String LABELS_IMAGE_NAME = "setagaya.jpg";
  private static final String LANDMARKS_IMAGE_NAME = "st_basils.jpg";
  private static final String LOGOS_IMAGE_NAME = "google_logo.jpg";
  private static final String OBJECT_IMAGE_NAME = "fallon-michael-8LKQfBumjMo.jpg";
  private static final String PROPERTIES_IMAGE_NAME = "bali.jpg";
  private static final String SAFE_SEARCH_IMAGE_NAME = "keyur-nandaniya-oEgiJNbYw8w.jpg";
  private static final String TEXT_IMAGE_NAME = "sign.jpg";
  private static final String WEB_DETECTION_IMAGE_NAME = "carnaval.jpg";

  private static final String CROP_HINTS_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + CROP_HINTS_IMAGE_NAME;
  private static final String FACE_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + FACE_IMAGE_NAME;
  private static final String LABELS_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + LABELS_IMAGE_NAME;
  private static final String LANDMARKS_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + LANDMARKS_IMAGE_NAME;
  private static final String LOGOS_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + LOGOS_IMAGE_NAME;
  private static final String OBJECTS_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + OBJECT_IMAGE_NAME;
  private static final String PROPERTIES_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + PROPERTIES_IMAGE_NAME;
  private static final String SAFE_SEARCH_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + SAFE_SEARCH_IMAGE_NAME;
  private static final String TEXT_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET + "/"
          + TEXT_IMAGE_NAME;
  private static final String WEB_DETECTION_IMAGE_FILE_PATH = BUCKET_NAME + "/" + IMAGE_PATH_IN_BUCKET
          + "/" + WEB_DETECTION_IMAGE_NAME;

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

  private static final String FOLDER_CONTENT_TYPE = "application/x-www-form-urlencoded;charset=UTF-8";

  private static void createFolder(String folderName) throws FileNotFoundException {
    System.out.println("Creating folder: " + folderName);
    FileInputStream fileInputStream = new FileInputStream("examples/empty_file.txt");
    bucket.create(folderName + "/",
            fileInputStream,
            FOLDER_CONTENT_TYPE,
            Bucket.BlobWriteOption.doesNotExist());
  }

  private static void uploadImages() throws FileNotFoundException {
    ArrayList<String> allImages = new ArrayList<>(10);
    allImages.add(CROP_HINTS_IMAGE_NAME);
    allImages.add(FACE_IMAGE_NAME);
    allImages.add(LABELS_IMAGE_NAME);
    allImages.add(LANDMARKS_IMAGE_NAME);
    allImages.add(LOGOS_IMAGE_NAME);
    allImages.add(OBJECT_IMAGE_NAME);
    allImages.add(PROPERTIES_IMAGE_NAME);
    allImages.add(SAFE_SEARCH_IMAGE_NAME);
    allImages.add(TEXT_IMAGE_NAME);
    allImages.add(WEB_DETECTION_IMAGE_NAME);

    for (String imageName : allImages) {
      String localImagePath = LOCAL_IMAGE_FOLDER_PATH + "/" + imageName;

      FileInputStream fileInputStream = new FileInputStream(localImagePath);
      System.out.println("Uploading " + localImagePath + " to " + bucket.getName());
      bucket.create(IMAGE_PATH_IN_BUCKET + "/" + imageName,
              fileInputStream,
              JPG_CONTENT_TYPE,
              Bucket.BlobWriteOption.doesNotExist());
    }
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
    System.out.println("Deleting bucket: " + bucket.getName());
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

    GCSPath path = GCSPath.from(BUCKET_NAME);
    String bucketName = path.getBucket();
    bucket = storage.get(bucketName);
    if (bucket != null) {
      deleteBucket(storage, bucket);
    }

    System.out.println("Creating bucket: " + bucketName);
    BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName)
            .setStorageClass(StorageClass.STANDARD)
            .setLocation("us-east1")
            .build();
    bucket = storage.create(bucketInfo);
    uploadImages();
  }

  @After
  public void destroy() {
    deleteBucket(storage, bucket);
  }

  @Test
  public void testRunCropHintsFeature() throws Exception {
    String folder = "crop_hints";
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunCropHintsFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunFaceFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunImagePropertiesFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunLabelsFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunLandmarksFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);

    createFolder(folder);

    System.out.println("testRunLogosFeature() - resultFolderPath: " + resultFolderPath);
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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunObjectLocalizationFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunTextFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunSafeSearchFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    String resultFolderPath = String.format(PATH_PATTERN, BUCKET_NAME, folder);
    System.out.println("testRunSafeSearchFeature() - resultFolderPath: " + resultFolderPath);

    createFolder(folder);

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
    Thread.sleep(10000); // Wait a bit to make sure the blob is available in the bucket
    String blobPath = String.format(RESULT_PATH_PATTERN, folder, RESULT_FILE_NAME);
    System.out.println("validateResult() - blobPath: " + blobPath);
    Blob blob = bucket.get(blobPath);
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

    Assert.assertTrue(expectedUri.equals(uri));
  }
}
