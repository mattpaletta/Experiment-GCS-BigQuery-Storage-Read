## Experiment: GCS Bigquery Storage Read

To get gcloud:
```bash
curl https://sdk.cloud.google.com | bash
gcloud auth application-default login
```

## Example Run
```
ninja && ./test_download <gcp_project_id> "projects/bigquery-public-data/datasets/baseball/tables/schedules"
```