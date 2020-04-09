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

package io.cdap.plugin.cloud.vision;

import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import io.cdap.plugin.cloud.vision.action.GcsBucketHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

/**
 * Test that we can access blobs using a GCSPath and that the batching computation doesn't trigger
 * an exception.
 */
public class GcsBucketHelperTest {
  protected static final String PATH = System.getProperty("path", "gs://vision-api-pbo2/images");
  protected static final String SERVICE_ACCOUNT_FILE_PATH = System.getProperty("serviceFilePath", "auto-detect");
  // This is a limit imposed by the Cloud Vision API
  protected static final int MAX_NUMBER_OF_IMAGES_PER_BATCH = 2000;

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void getAllFilesInPath() throws Exception {
    Credentials credentials = CredentialsHelper.getCredentials(SERVICE_ACCOUNT_FILE_PATH);

    List<Blob> blobs = GcsBucketHelper.getAllFilesInPath(PATH, credentials);
    if (blobs.isEmpty()) {
      throw new Exception("Cannot get blobs in: " + PATH);
    }

    System.out.println();
    System.out.println("Using service acount path: " + SERVICE_ACCOUNT_FILE_PATH);
    System.out.println("Blobs found in: " + PATH);
    for (Blob blob : blobs) {
      System.out.println("blob.getName(): " + blob.getName());
    }

    int countBlobs = 0;
    for (int batchId = 0; batchId < (1 + blobs.size() / MAX_NUMBER_OF_IMAGES_PER_BATCH); batchId++) {
      for (int index = batchId * MAX_NUMBER_OF_IMAGES_PER_BATCH;
           (index < (batchId + 1) * MAX_NUMBER_OF_IMAGES_PER_BATCH) && (index < blobs.size());
           index++) {
        countBlobs++;
      }
    }

    // Make sure we have taken into the loop as many blobs as returned from GCS
    Assert.assertEquals(countBlobs, blobs.size());
  }
}
