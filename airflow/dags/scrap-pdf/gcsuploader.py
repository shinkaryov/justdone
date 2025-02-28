import time
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError


class GCSUploader:
    """
    A class for uploading text files to Google Cloud Storage (GCS).

    Attributes:
        bucket_name (str): The name of the GCS bucket.
        client (storage.Client): The Google Cloud Storage client.
        bucket (storage.Bucket): The bucket object.
    """

    def __init__(self, bucket_name):
        """
        Initializes the GCSUploader with a specified bucket.

        Args:
            bucket_name (str): The name of the GCS bucket.
        """
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_text(self, file_name, text, max_retries=3, delay=2):
        """
        Uploads a text file to GCS with up to 3 retries.

        Args:
            file_name (str): The name of the file in GCS.
            text (str): The text content to upload.
            max_retries (int, optional): The maximum number of retry attempts. Defaults to 3.
            delay (int, optional): The initial delay in seconds before retrying. Defaults to 2.
        """
        blob = self.bucket.blob(file_name)

        for attempt in range(1, max_retries + 1):
            try:
                blob.upload_from_string(text, content_type="text/plain")
                print(f"✅ File {file_name} has been uploaded to GCS.")
                break
            except GoogleAPIError as e:
                print(f"⚠️ Attempt {attempt} failed to upload {file_name}: {e}")
                if attempt < max_retries:
                    time.sleep(delay * (2 ** (attempt - 1)))  # Exponential backoff
                else:
                    print(f"❌ Failed to upload {file_name} after {max_retries} attempts.")
