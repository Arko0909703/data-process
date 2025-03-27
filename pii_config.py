
from config import *
import hashlib
import uuid
from functools import partial
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.config import Config
from boto3.dynamodb.conditions import Key
import pandas as pd
import gc
# OUTPUT_DATA_BUCKET = 'ph-etl-archive'
# output_log_key="pravin-testing-etl/Test/"

dynamodb_resource = boto3.resource('dynamodb', config=Config(
    region_name='ap-south-1',
    max_pool_connections=100,
    retries={'max_attempts': 10, 'mode': 'standard'}
))

# AWS Configurations
dynamodb_client_config = Config(
    region_name='ap-south-1',
    max_pool_connections=100,
    retries={'max_attempts': 10, 'mode': 'standard'}
)
dynamodb_client = boto3.client('dynamodb', config=dynamodb_client_config)

# customer_table = dynamodb_resource.Table('Customer-pii-test_2')
# customer_table = dynamodb_resource.Table('Customer-pii-test')

# Locks for thread safety
existing_records_lock: Lock = Lock()
memoization_lock: Lock = Lock()
memoization_cache_lock= Lock()


# Data structure for memoization
# memoization_cache = {}

ENCRYPTED_FIELDS = {
    "Mobile_Number": 0,
    "Customer_Name": 1,
    "Latitude": 2,
    "Longitude": 3,
    "Email": 4,
    "Customer_Address": 5,
    "City": 6
}

# Define normal fields
NORMAL_FIELDS = {
    "Header_Id": 0,
    "Customer_Id": 1,
    "Franchise": 2,
    "Store_No": 3,
    "Enriched_Order_Date": 4,
    "Enriched_Order_Time": 5,
    "Receipt_No": 6,
    "Valid":7,
    "Unsubscribed":8,
    "Unsubscribe_Date":9

}

# SELECTED_COLUMNS = ["Mobile_Number", "Valid", "Order_Date"]
SELECTED_COLUMNS = ["Customer_Id","Encrypted_Mobile_Number", "Valid", "Order_Date","Unsubscribed","Unsubscribe_Date"]

# PII_COLUMNS = ["Mobile_Number", "Valid", "Customer_Name", "Customer_Id", "Email", "Order_Date", "Address",
#                "Store_Code", "Order_Id", "Latitude", "Longitude", "City", "Order_Time", "Header_Id", "Franchise"]

PII_COLUMNS = ["Mobile_Number", "Valid", "Customer_Name", "Customer_Id", "Email", "Order_Date", "Address",
               "Store_Code", "Order_Id", "Latitude", "Longitude", "City", "Order_Time",
                 "Header_Id", "Franchise","Unsubscribed","Unsubscribe_Date"]


raw_to_pii_column_rename_mapping = {'Mobile_Number':'Mobile_Number', 
                                    'Valid':'Valid', 
                                    'Header_Id':"Header_Id",
                                    "Customer_Name":"Customer_Name",
                                    "Enriched_Email_Id":"Email",
                                    "Enriched_Order_Date":"Order_Date",
                                    'Enriched_Order_Time':'Order_Time',
                                    'Store_No':'Store_Code',
                                    'Receipt_No':'Order_Id',
                                    'Store_City':'City',
                                    'Franchise':'Franchise', 
                                    'Address':'Address',
                                    'PHDV_Latitude':'Latitude',
                                    'PHDV_Longitude':'Longitude'
                                    }

pii_column_renaming={
    "Mobile_Number":"Mobile_Number",
    "Valid":"Valid",
    "Customer_Name":"Customer_Name",
    "Customer_Id":"Customer_Id",
    "Email":"Email",
    "Order_Date":"Enriched_Order_Date",
    "Address":"Customer_Address",
    "Store_Code":"Store_No",
    "Order_Id":"Receipt_No",
    "Latitude":"Latitude",
    "Longitude":"Longitude",
    "City":"City",
    "Order_Time":"Enriched_Order_Time",
    "Header_Id":"Header_Id",
    "Franchise":"Franchise",
    "Unsubscribed":"Unsubscribed",
    "Unsubscribe_Date":"Unsubscribe_Date"
}




CHUNK_SIZE = 25

# CUSTOMER_TABLE = 'Customer_Prod'
# PII_TABLE = "Order_PII_Prod"
# MAPPING_TABLE = "Mapping_Table_Prod"

CUSTOMER_TABLE = 'Customer_prod_test'
PII_TABLE = "PII_prod_test"
MAPPING_TABLE = "Mapping_prod_test"

# CUSTOMER_TABLE = 'Customer_dil'
# PII_TABLE = "PII_dil"
# MAPPING_TABLE = "Mapping_dil"

# MAX_WORKERS = 10


# DATA_DICT_MAPPING = {
#     "Header_Id": 0,
#     "Customer_Id": 1,
#     "Mobile_Number": 2,
#     "Customer_Name": 3,
#     "Latitude": 4,
#     "Longitude": 5,
#     "Email": 6,
#     "Customer_Address": 7,
#     "City": 8,
#     "Franchise": 9,
#     "Store_No": 10,
#     "Enriched_Order_Date": 11,
#     "Enriched_Order_Time": 12,
#     "Receipt_No": 13,
#     "Valid": 14
# }
def get_max_vcpus(job_name):
    # Create a Glue client
    client = boto3.client('glue')
    try:
        # Retrieve job details
        response = client.get_job(JobName=job_name)
        job_details = response['Job']
        # Extract worker type and number of workers
        worker_type = job_details.get('WorkerType', 'Standard')
        num_workers = job_details.get('NumberOfWorkers', 1)
        # Determine vCPUs per worker based on worker type
        if worker_type == 'G.1X':
            vcpus_per_worker = 4
        elif worker_type == 'G.2X':
            vcpus_per_worker = 8
        else:
            vcpus_per_worker = 4  # Default to Standard or unknown types
        # Calculate total vCPUs
        vcpus = num_workers * vcpus_per_worker
        return vcpus
    except Exception as e:
        logger(f"Error retrieving job details: {e}")
        return None 
    
def get_max_workers(default_workers = 10) :
    try:
        cpu_count = multiprocessing.cpu_count()
        max_workers = min(cpu_count * 2, default_workers)
        return max_workers
    except Exception as e:
        logger.error(f"Error determining max workers: {e}")
        return default_workers 
    
# AWS Configuration
AWS_REGION = 'ap-south-1'
AWS_MAX_POOL_CONNECTIONS = 100
AWS_MAX_ATTEMPTS = 10

# Threading Configuration
THREAD_POOL_WORKERS = get_max_workers()

# Update Columns
UPDATE_COLUMNS = ['Customer_Id',
    'Aggregator_Id',
    'Store_No',
    'Channel',
    'Source',
    'Fullfillment',
    'Online_ref_ID',
    'Reference_No',
    'Receipt_No',
    'Transaction_No',
    'Invoice_No',
    'Enriched_Order_Date',
    'Enriched_Order_Time',
    'Franchise',
    'Pizzahut_Brand']

# Batch Size for DynamoDB operations
BATCH_SIZE = 25

# Schema for Spark DataFrame
SPARK_SCHEMA = [
    ('Header_Id', 'StringType'),
    ('Customer_Id', 'StringType'),
    ('Aggregator_Id', 'StringType'),
    ('Store_No', 'StringType'),
    ('Channel', 'StringType'),
    ('Source', 'StringType'),
    ('Fullfillment', 'StringType'),
    ('Online_ref_ID', 'StringType'),
    ('Reference_No', 'StringType'),
    ('Receipt_No', 'StringType'),
    ('Transaction_No', 'StringType'),
    ('Invoice_No', 'StringType'),
    ('Enriched_Order_Date', 'StringType'),
    ('Enriched_Order_Time', 'StringType'),
    ('Franchise', 'StringType'),
    ('Pizzahut_Brand', 'StringType'),
]

schema = StructType([
            StructField("Header_Id", StringType(), True),
            StructField("Customer_Id", StringType(), True),
            StructField("Aggregator_Id", StringType(), True),
            StructField("Store_No", StringType(), True),
            StructField("Channel", StringType(), True),
            StructField("Source", StringType(), True),
            StructField("Fullfillment", StringType(), True),
            StructField("Online_ref_ID", StringType(), True),
            StructField("Reference_No", StringType(), True),
            StructField("Receipt_No", StringType(), True),
            StructField("Transaction_No", StringType(), True),
            StructField("Invoice_No", StringType(), True),
            StructField("Enriched_Order_Date", StringType(), True),
            StructField("Enriched_Order_Time", StringType(), True),
            StructField("Franchise", StringType(), True),
            StructField("Pizzahut_Brand", StringType(), True)
        ])


def log_dataframe_size(df, name):
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    size_in_mb = size_in_bytes / (1024 * 1024)
    size_in_gb = size_in_bytes / (1024 * 1024 * 1024)
    # Log the size in MB and GB
    print(f"Size in MB: {size_in_mb:.2f} MB")
    print(f"Size in GB: {size_in_gb:.2f} GB")
    # logger.info(df.explain(mode='cost'))
 
# Example usage:
# Assuming you have a Spark DataFrame 'df'
# log_dataframe_size(df)

import psutil
def log_available_memory():
    memory_info = psutil.virtual_memory()
    # Available memory in bytes
    available_memory = memory_info.available
    # Convert to MB for readability
    available_memory_mb = available_memory / (1024 * 1024)
 
    logger.info(f"Available Memory: {available_memory_mb:.2f} MB")



