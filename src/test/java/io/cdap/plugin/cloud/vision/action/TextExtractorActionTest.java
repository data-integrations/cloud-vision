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
import io.cdap.plugin.cloud.vision.CredentialsHelper;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Test class for {@link TextExtractorAction}.
 */
public class TextExtractorActionTest {
  protected static final String PROJECT = System.getProperty("project", "auto-detect");
  protected static final String SERVICE_ACCOUNT_FILE_PATH = System.getProperty("serviceFilePath", "auto-detect");
  protected static final String PATH = System.getProperty("path", "gs://cloud-vision-cdap-text-offline");
  protected static final String PATH_PATTERN = "%s/%s";

  protected static final String PDF_CONTENT_TYPE = "application/pdf";
  protected static final String PDF_FILE_NAME = "sample.pdf";
  protected static final String PDF_FILE_PATH = String.format(PATH_PATTERN, PATH, PDF_FILE_NAME);
  protected static final String RESULT_FOLDER_PATH = String.format(PATH_PATTERN, PATH, "/");
  protected static final String RESULT_FILE_NAME = "output-1-to-1.json";

  protected static final int ATTEMPTS_AMOUNT = 30;
  protected static final int ATTEMPTS_DELAY = 1000;

  private static Storage storage;
  private static Bucket bucket;

  private static void initTestsData() throws FileNotFoundException {
    String path = String.format("src/test/resources/%s", PDF_FILE_NAME);

    FileInputStream serviceAccountStream = new FileInputStream(path);
    bucket.create(PDF_FILE_NAME, serviceAccountStream, PDF_CONTENT_TYPE, Bucket.BlobWriteOption.doesNotExist());
  }

  private static Storage getStorage(String project, @Nullable Credentials credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      builder.setCredentials(credentials);
    }
    return builder.build().getService();
  }

  private static void deleteBucket(Storage storage, Bucket bucket) {
    if (bucket == null || bucket.list() == null)
      return;
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
  public void testRun() throws Exception {
    TextExtractorActionConfig config = new TextExtractorActionConfig(
            SERVICE_ACCOUNT_FILE_PATH,
            PDF_FILE_PATH,
            RESULT_FOLDER_PATH,
            PDF_CONTENT_TYPE,
            1,
            null
    );

    TextExtractorAction action = new TextExtractorAction(config);
    ActionContext context = new MockActionContext();
    action.run(context);

    // Waiting for results
    Blob blob = null;
    for (int i = 0; i < ATTEMPTS_AMOUNT; i++) {
      blob = bucket.get(RESULT_FILE_NAME);
      if (blob != null) {
        break;
      }
      Thread.sleep(ATTEMPTS_DELAY);
    }

    Assert.assertNotNull(blob);
    Assert.assertTrue(blob.exists());

    String content = new String(blob.getContent());
    JsonParser parser = new JsonParser();
    JsonElement jsonTree = parser.parse(content);
    String text = jsonTree.getAsJsonObject()
            .get("responses")
            .getAsJsonArray()
            .get(0)
            .getAsJsonObject()
            .get("fullTextAnnotation")
            .getAsJsonObject()
            .get("text")
            .getAsString();
    Assert.assertEquals("Hello World!\n", text);
  }
}
