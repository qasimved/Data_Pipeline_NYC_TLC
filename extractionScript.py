import os,sys
from pprint import pprint
from google.cloud import storage
from datetime import date, timedelta,datetime
from dateutil.relativedelta import *
import requests

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(sys.path[0], "dpl-elt-service-key.json")

storage_client = storage.Client()

staging_bucket = 'dpl-elt-staging'
local_data_bucket = 'dpl-elt-local'
semantic_data_bucket = 'dpl-elt-semantic'

source_yellow_tripdata_base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
source_green_tripdata_base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/" 

# Function to setup 3-layer architecture with respect to buckets - checks if project have 3 buckets for 3 layers and create if not present
def setup_buckets():
    # getting list of all avialable buckets under this project
    bucket_list = []
    for bucket_item in storage_client.list_buckets(max_results=100):
        bucket_list.append(bucket_item.name)
    
    # check and create the 3 required buckets
    if staging_bucket not in bucket_list:
         # create a new bucket for staging data layer
        bucket = storage_client.bucket(staging_bucket)
        bucket.storage_class = 'STANDARD' # ARCHIVE | NEARLINE | STANDARD | COLDLINE
        bucket.location = 'US' 
        storage_client.create_bucket(bucket) 
    
    if local_data_bucket not in bucket_list:
        # create a new bucket for local data layer
        bucket = storage_client.bucket(local_data_bucket)
        bucket.storage_class = 'STANDARD' # ARCHIVE | NEARLINE | STANDARD | COLDLINE
        bucket.location = 'US' 
        storage_client.create_bucket(bucket) 

    if semantic_data_bucket not in bucket_list:
        # create a new bucket for semantic data layer
        bucket = storage_client.bucket(semantic_data_bucket)
        bucket.storage_class = 'STANDARD' # ARCHIVE | NEARLINE | STANDARD | COLDLINE
        bucket.location = 'US' 
        storage_client.create_bucket(bucket) 

# Function to get the data for specific range (start and end date month and year format) and upload that data to staaging layer bucket 
def extract_data_with_range(start_date_str,end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m')
    end_date = datetime.strptime(end_date_str, '%Y-%m')
    month_count = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month
    month_count = month_count+1
    for counter in range(month_count):
        source_file_url = custom_source_url_generator(start_date,counter,"yellow")
        destination_file_name = custom_destination_name_generator(start_date,counter,"yellow")
        print("Getting file for yellow texi from the source: "+source_file_url)
        # Getting file for yellow texi from the source
        requestObj = requests.get(source_file_url, allow_redirects=True)
        open(destination_file_name, 'wb').write(requestObj.content)
        bucket = storage_client.get_bucket(staging_bucket)
        # Uploading file for yellow texi to the staging bucket
        print("Uploading file for yellow texi to the staging bucket")
        blob = bucket.blob(custom_destination_name_generator(start_date,counter,"yellow"))
        # blob.upload_from_filename(destination_file_name, content_type='csv')
        print("Upload successfull: "+destination_file_name)
        # os.remove(destination_file_name)

        source_file_url = custom_source_url_generator(start_date,counter,"green")
        destination_file_name = custom_destination_name_generator(start_date,counter,"green")
        print("Getting file for green texi from the source: "+source_file_url)
        # Getting file for green texi from the source
        requestObj = requests.get(source_file_url, allow_redirects=True)
        open(destination_file_name, 'wb').write(requestObj.content)
        bucket = storage_client.get_bucket(staging_bucket)
        # Uploading file for green texi to the staging bucket
        print("Uploading file for green texi to the staging bucket")
        blob = bucket.blob(custom_destination_name_generator(start_date,counter,"green"))
        # blob.upload_from_filename(destination_file_name, content_type='csv')
        print("Upload successfull: "+destination_file_name)
        # os.remove(destination_file_name)


def custom_source_url_generator(start_date, month_counter, source_type):
    latest_date = start_date + relativedelta(months=month_counter)
    if source_type.lower()=='yellow':
        return source_yellow_tripdata_base_url +"yellow_tripdata_"+ latest_date.strftime("%Y-%m")+".csv"
    elif source_type.lower()=='green':
        return source_green_tripdata_base_url + "green_tripdata_"+latest_date.strftime("%Y-%m")+".csv"

def custom_destination_name_generator(start_date, month_counter, source_type):
    latest_date = start_date + relativedelta(months=month_counter)
    if source_type.lower()=='yellow':
        return "yellow_tripdata_" + latest_date.strftime("%Y-%m")+".csv"
    elif source_type.lower()=='green':
        return "green_tripdata_" + latest_date.strftime("%Y-%m")+".csv"




setup_buckets()
extract_data_with_range("2021-1","2021-3")