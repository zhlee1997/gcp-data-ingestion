# If not already installed, do: pip install pandas fastparquet
import datetime
import pandas as pd
from fastparquet import write
from google.cloud import storage

# Get the current date
current_date = datetime.datetime.now()

# Calculate the date 30 days ago
thirty_days_ago = current_date - datetime.timedelta(days=30)

# year = datetime.datetime.now().strftime('%Y')
# month = datetime.datetime.now().strftime('%m')
# day = datetime.datetime.now().strftime('%d')

# Get the month from the date 30 days ago
year_thirty_days_ago = thirty_days_ago.strftime('%Y')
month_thirty_days_ago = thirty_days_ago.strftime('%m')
day_thirty_days_ago = thirty_days_ago.strftime('%d')

filter_datetime = thirty_days_ago.strftime('%Y-%m-%d')

def price_catcher_pipeline(event, context):
    URL_DATA = f'https://storage.data.gov.my/pricecatcher/pricecatcher_{year_thirty_days_ago}-{month_thirty_days_ago}.parquet'

    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])

    print(df)
    filter_df = df[df['date'] == filter_datetime]

    # Save the parquet file locally
    local_file_path = f'/tmp/pricecatcher_{year_thirty_days_ago}-{month_thirty_days_ago}.parquet'
    filter_df.to_parquet(local_file_path)

    # Save the parquet file to Cloud Storage
    bucket_name = 'bdaa-assignment-parquet-bucket'
    bdaa_bucket = storage.Client().get_bucket(bucket_name)
    # bdaa_bucket.blob('parquet-file/pricecatcher-{0}.csv'.format(datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S'))).upload_from_string(df.to_csv(), "text/csv")
    # bdaa_bucket.blob('parquet-file/pricecatcher-{0}.parquet'.format(thirty_days_ago.strftime('%Y-%m-%d %H_%M_%S'))).upload_from_string(df.to_parquet(), "text/parquet")

    blob_name = 'parquet-file/pricecatcher-{0}.parquet'.format(thirty_days_ago.strftime('%Y-%m-%d %H_%M_%S'))
    bdaa_bucket.blob(blob_name).upload_from_filename(local_file_path)

    print("Parquet file saved to Cloud Storage!")
    print(f"Number of rows saved in parquet file: {len(filter_df)}")

# price_catcher_pipeline("", "")