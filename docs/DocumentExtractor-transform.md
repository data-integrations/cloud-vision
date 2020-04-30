# Google Cloud Document Extractor Transform

Description
-----------
This transform plugin uses the Google Cloud Vision API to detect and transcribe text from (up to 5 pages) documents 
(.pdf, .tiff, .gif) files stored in Google Cloud Storage.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Google Cloud Platform Console.

Make sure the account key has permission to access the Google Cloud Vision API.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project. It can be found on the Dashboard in the
Google Cloud Platform Console.

**Path Field**: Name of the field in the input schema containing the path to the image.

**Content Field**: Name of the field in the input schema containing the file content, represented as a stream of bytes.

Either 'Path Field' or 'Content Field' must be not null. They are mutually exclusive and cannot be both specified.

**Output Field**: Name of the field to store the extracted image features into. If the specified output field name 
already exists in the input record, it will be overwritten.

**Mime Type**: The type of the file(s) that will be processed. Currently only 'application/pdf', 'image/tiff' and 
'image/gif' are supported. Wildcards are not supported.

**Pages**: The list of pages in the file(s) to perform image annotation on. Enter the list as Comma Separated Values.

**Features**: Features to extract from the documents.

**Language Hints**: Hints to detect the language of the text in the documents.

**Aspect Ratios**: Ratio of the width to the height of the image. If not specified, the best possible crop is returned.

**Include Geo Results**: Whether to include results derived from the geo information in the document.
