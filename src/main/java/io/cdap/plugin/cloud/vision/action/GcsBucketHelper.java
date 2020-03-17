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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;


/**
 * Helper class used to retrieve a list of Blobs from a GCSPath.
 */

public class GcsBucketHelper {
  // Logging
  private static final Logger LOG = LoggerFactory.getLogger(GcsBucketHelper.class);

  /**
   * Helper function that returns a List of Blobs that are found in a given path on GCS.
   *
   * @param sourceFolderPath
   * @param credentials
   * @return List of Blobs
   */
  public static List<Blob> getAllFilesInPath(String sourceFolderPath, Credentials credentials) {
    List<Blob> results = new ArrayList<>();

    Storage storage = StorageOptions.newBuilder().setCredentials(credentials)
            .build().getService();

    // Loop through the buckets
    for (Bucket currentBucket : storage.list().iterateAll()) {
      // Loop though the blobs and check if the path is what we want, based on sourceFolderPath
      for (Blob blob : currentBucket.list().iterateAll()) {
        if (blob.getName().endsWith("/")) { // It's a folder
          continue; // Ignore
        }
        // Rebuild the full path of the blob
        String fullBlobPath = "gs://" + blob.getBucket() + "/" + blob.getName();
        // Is the blob part of the path we have been given?
        if (fullBlobPath.startsWith(sourceFolderPath)) {
          results.add(blob);
        }
      }
    }
    return results;
  }
}
