import pandas as pd 
from google.cloud import storage
from google.cloud import bigquery
import os
import io
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/magic-zoomcamp/config/de_zoomcamp_final_project.json"

# Information
bucket_name = 'de_zoomcamp_final_project'
data_name = 'ecommerce_electric_data'
dataset_id = bucket_name
table_id = data_name

def format_full_int(x):
    return '{:.0f}'.format(x)

@custom
def upload_to_bigquery(*args, **kwargs):
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Read CSV data from GCS into a DataFrame
    blobs = storage_client.list_blobs(bucket_name, prefix=f'{data_name}/202')
    dfs = []
    for blob in blobs:
        if blob.name.endswith('.csv'):  # Filter only CSV files
            # Read the CSV file from GCS
            blob_data = blob.download_as_string()
            df = pd.read_csv(io.BytesIO(blob_data))
            df['event_date'] = pd.to_datetime(df['event_date']).dt.date  # Convert to date
            df['event_time'] = pd.to_datetime(df['event_time'])          # Convert to datetime
            df['order_id'] = df['order_id'].apply(format_full_int)                 # Convert to string
            df['product_id'] = df['product_id'].apply(format_full_int)             # Convert to string
            df['category_id'] = df['category_id'].apply(format_full_int)           # Convert to string
            df['category_code'] = df['category_code'].astype(str)        # Convert to string
            df['brand'] = df['brand'].astype(str)                        # Convert to string
            df['user_id'] = df['user_id'].apply(format_full_int)

            df = df.dropna(subset=['price'])
            df['category_id'] = df['category_id'].replace('nan','others')
            df['category_code'] = df['category_code'].replace('nan','others')
            df['brand'] = df['brand'].replace('nan','others')

            split_category = df['category_code'].str.split('.', expand=True)
            split_category.columns = ['category', 'subcategory', 'product_name']
            df ['category'] = split_category['category'] 
            df ['subcategory'] = split_category['subcategory']
            df ['product_name'] = split_category['product_name']
            dfs.append(df)
    final_df = pd.concat(dfs, ignore_index=True)

    # Upload DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[{"name": "event_date", "type": "DATE"},
                {"name": "event_time", "type": "DATETIME"},
                {"name": "order_id", "type": "STRING"},
                {"name": "product_id", "type": "STRING"},
                {"name": "category_id", "type": "STRING"},
                {"name": "category_code", "type": "STRING"},
                {"name": "brand", "type": "STRING"},
                {"name": "price", "type": "FLOAT64"},
                {"name": "user_id", "type": "STRING"},
                {"name": "category", "type": "STRING"},
                {"name": "subcategory", "type": "STRING"},
                {"name": "product_name", "type": "STRING"}],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,  # Create table if not exists
        time_partitioning=bigquery.TimePartitioning(field="event_date")  # Partition by date_column
    )

    table_ref = bq_client.dataset(dataset_id).table(table_id)
    job = bq_client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
    job.result()  # Wait for job completion

    print(f"Uploaded to BigQuery table {dataset_id}.{table_id} partitioned by date_column")
