# data-validation-framework
This sample code is intended for use in large scale data migrations to Google Cloud (specifically, BigQuery). The solution offers a reusable and scalable way to validate a large number of tables, as well as automated solutions for validating extremely large tables with minimal manual intervention.

## Steps to implement framework in your GCP project:

1. Fork source code from GitHub
2. Create Cloud Build trigger (point to cloudbuild.yaml file in repo) to deploy Docker images to Artifact Registry
    * dvt-cloud-run-execute (primary executor)
    * dvt-cloud-run-partition-execute (secondary executor)
3. Create Cloud Run jobs for primary and secondary executor and assign respective Docker images
4. Machine size depends on size of tables needing validation
    * Recommend at least 1GiB+1CPU for primary and 2GiB+1CPU for secondary 
5. Upload config file to GCS bucket
6. Upload custom SQL scripts (as needed) to GCS bucket
