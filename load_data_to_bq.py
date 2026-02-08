from google.cloud import bigquery

def run_bq_load():
    client = bigquery.Client(project="project-248894f0-fba6-4a51-95b")
    
    table_id = "project-248894f0-fba6-4a51-95b.trips_data.yellow_tripdata_2024"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, 
            field="tpep_pickup_datetime"             
        )
    )
    
    gcs_uri = "gs://hw3_2025/yellow_tripdata_2024-*.parquet"
    
    print(f"🚀 Starting the job: {gcs_uri}")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    
    load_job.result()  # 等待加载完成
    
    table = client.get_table(table_id)
    print(f"✅ Sucess! Load {table.num_rows} Rows。")
    if table.time_partitioning:
        print(f"📊 Partation {table.time_partitioning.field} successfully。")

if __name__ == "__main__":
    run_bq_load()
