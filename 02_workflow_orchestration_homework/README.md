# Steps to Load NY Taxi Trip Data in Google Cloud

To load the NY taxi trip data 2019-2021 in Google Cloud, follow these steps:

1. **Create the Project and Service Account in GCP**  
   Set up a project and service account in Google Cloud Platform (GCP).

2. **Modify the `gcp_kv.yaml` File**  
   Update the `gcp_kv.yaml` file with the key from the service account. Assign the GCS bucket name and dataset name. 

3. **Run the `gcp_kv.yaml` Workflow in Kestra**  
   Execute the `gcp_kv.yaml` workflow in Kestra to configure the GCP credentials. Becasue of the sensitive information included, the `gcp_kv.yaml` File is ignored in the push. 

4. **Run the `gcp_setup.yaml` Workflow**  
   Execute the `gcp_setup.yaml` workflow to create the required GCS bucket and dataset. See details in `gcp_setup.yaml` file. 

5. **Set Up the `gcp_taxi_scheduled.yaml` Workflow**  
   Configure and schedule the `gcp_taxi_scheduled.yaml` workflow in Kestra. See details in `gcp_taxi_scheduled.yaml`file. 

6. **Configure Data Backfill in the Triggers Tab**  
   Set up data backfill for both green taxi and yellow taxi datasets in the Triggers tab. Ensure data is backfilled for the years 2019 to 2021.

7. **Compute the answers for questions 3-5 in Bigquery**
    ```sql
    select count(*)
    from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata`
    where filename like 'yellow_tripdata_2021-%.csv'

    select count(*)
    from `data-engineer-zoomcamp-2025.zoomcamp.green_tripdata`
    where filename like 'green_tripdata_2021-%.csv'

    select count(*)
    from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata`
    where filename like 'yellow_tripdata_2021-03.csv'