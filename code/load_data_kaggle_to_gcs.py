import pandas as pd 
from google.cloud import storage
import os
import io
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/magic-zoomcamp/config/de_zoomcamp_final_project.json"

# Information
file_path = '/home/src/magic-zoomcamp/data/ecommerce_electric_data.csv'
bucket_name = 'de_zoomcamp_final_project'
data_name = 'ecommerce_electric_data'

#Load data to GCS
def format_full_int(x):
    return '{:.0f}'.format(x)

@custom
def upload_to_GCS (*args, **kwargs):
    storage_client = storage.Client ()
    data = pd.read_csv (file_path)
    # Add event_date column to partition the files
    data['event_time'] = pd.to_datetime(data['event_time'])
    data['event_date'] = data['event_time'].dt.date
    column_order = ['event_date'] + [col for col in data.columns if col != 'event_date']
    data = data[column_order]
    date_partition = data['event_date'].unique()

    # Data types refine
    data['order_id'] = data['order_id'].apply(format_full_int)                 # Convert to string
    data['product_id'] = data['product_id'].apply(format_full_int)             # Convert to string
    data['category_id'] = data['category_id'].apply(format_full_int)           # Convert to string
    data['category_code'] = data['category_code'].astype(str)        # Convert to string
    data['brand'] = data['brand'].astype(str)                        # Convert to string
    data['user_id'] = data['user_id'].apply(format_full_int)

    # Upload CSV data to GCS
    for date in date_partition:
        df_date = data[data['event_date'] == date]
        date_str = str(date)
        gcs_file_path = f"{data_name}/{date_str}.csv"
        csv_data = df_date.to_csv(index=False)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)
        blob.upload_from_string(csv_data, if_generation_match=None)
        print(f"File uploaded to GCS: gs://{bucket_name}/{gcs_file_path}")
