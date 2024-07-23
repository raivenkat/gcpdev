from google.cloud import storage
from google.oauth2.credentials import Credentials
import json

# Your JSON credentials as a string
identity = json.loads("""{
  "account": "",
  "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
  "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
  "quota_project_id": "dev-de-training",
  "refresh_token": "1//04rPaWOhp5HCeCgYIARAAGAQSNwF-L9IrXb7RfjSzlbXPwPdZmcODeZczu40lOxssZSVkUCOMpZTFhKW3jw8knBp1ZHDbapvHykg",
  "type": "authorized_user",
  "universe_domain": "googleapis.com"
}""")

# Create credentials from JSON info
gcp_credentials = Credentials.from_authorized_user_info(identity)

# Project and bucket details
project_id = "dev-de-training"
bucket_name = 'dev-de-training-default'
blob_name = 'data/RAW_ZONE/emp_data.csv'

# Create a storage client
gcsClient = storage.Client(project=project_id, credentials=gcp_credentials)

# Get the bucket
gcsBucket = gcsClient.bucket(bucket_name)

# Get the blob (file) from the bucket
gcs_blob = gcsBucket.blob(blob_name)

# Download the contents of the file as a string
file_contents = gcs_blob.download_as_text()

# Print the contents of the file
print(file_contents)

