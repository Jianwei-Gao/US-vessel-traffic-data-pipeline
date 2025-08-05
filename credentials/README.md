## Credentials

contains import credentials that should be gitignored \
I am using google service credential, to set up:
1. navigate to https://cloud.google.com/iam/docs/keys-create-delete 
2. follow to steps to create and download a service account key for your project (similar structure to "example-google-credentials.json")
3. move it into this folder (it will be gitignored) and rename it to "google-credential.json"
4. give the service account the following role (for example, through console -> IAM -> click the edit/pen symbol for the service account you will be using).
    * BigQuery Admin
    * Cloud Run Admin
    * Dataproc Administrator
    * Storage Admin
    * Storage Object Admin
    * Viewer
    * Service Account User

## do NOT upload/share with others the service account json you downloaded