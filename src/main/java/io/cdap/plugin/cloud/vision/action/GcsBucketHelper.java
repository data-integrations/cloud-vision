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
import io.cdap.plugin.cloud.vision.CredentialsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class GcsBucketHelper {
  // Logging
  private static Logger LOG = LoggerFactory.getLogger(GcsBucketHelper.class);

  /**
   * This is a helper function that returns a List of Blobs that are found in a given path on GCS.
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

  private void logStringAsBytes(String s, String methodName, String msg) {
    if (s == null)
      return;
    try {
      final byte[] arrayUtf8 = s.getBytes("UTF-8");
      StringBuilder sb = new StringBuilder(s.length() * 5);
      for (byte b : arrayUtf8)
        sb.append(b + " ");

      LOG.info("PATRICE: " + methodName + " " + msg + ": " + sb.toString());
    } catch (UnsupportedEncodingException e) {
      ;
    }
  }

  public static void main(String[] args) throws IOException {
    final String SERVICE_ACCOUNT_FILE_PATH = "/Volumes/RAID1/Jobs/Cirus/CDAP/gcp-keys/cdap-vision-test01-19b37b9ff15c.json";
    final String BUCKET = "gs://vision-api-pbo2/";

    System.out.println("SERVICE_ACCOUNT_FILE_PATH: " + SERVICE_ACCOUNT_FILE_PATH);

    String sourceFolderPath = BUCKET + "images";
    Credentials credentials = CredentialsHelper.getCredentials(SERVICE_ACCOUNT_FILE_PATH);

    List<Blob> res = GcsBucketHelper.getAllFilesInPath(sourceFolderPath, credentials);
    if (res == null) {
      System.out.println("No result");
      return;
    }

    System.out.println();
    System.out.println("Results:");
    for (Blob blob : res) {
      System.out.println("blob.getName(): " + blob.getName());
    }
  }
}
