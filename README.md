# Exchange Rates ETL Pipeline

A data pipeline for fetching, processing, and storing currency exchange rates in BigQuery.

## Quick Setup

NOTE: This instruction is not comprehensive, but if you have any doubts - feel free to ask.
You need to have GCP account, Python and pip installed, Docker and Docker Compose.

1. **GCP Setup**
   - Create GCP project: `gcloud projects create <project-id>`
   - Enable APIs: BigQuery API
   - Create service account with BigQuery Data Editor and BigQuery User roles
   - Download key as `service-account.json`

2. **Environment Setup**
   ```bash
   # Set environment variables
   export GCP_PROJECT_ID=<project-id>
   export GOOGLE_APPLICATION_CREDENTIALS=<path to service account key>
   ```

3. Create BigQuery Infrastructure with Terraform
```bash
cd terraform
terraform init
terraform apply
```

4. Initialize and run Airflow
```bash
make init
make up
```

5. Configure Airflow
    - Import variables from `airflow_variables.json` via Airflow UI (Admin -> Variables)
    - Add GCP connection via Airflow UI (Admin -> Connections)
        ```
        Connection Id: google_cloud_default
        Connection Type: Google Cloud
        Project ID: <your-project-id>
        Keyfile JSON: <content of your service key json>
        ```
    - Run your setup DAG, it'll create BigQuery dataset and table.

### Architecture

See [architecture](./docs/architecture.md) documentation for detailed design and component interactions.

Hope I didn't forget anything from the config process😅
