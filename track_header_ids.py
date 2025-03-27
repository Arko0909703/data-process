import boto3
from pii_config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,udf
from pyspark.sql.types import StringType
from concurrent.futures import ThreadPoolExecutor
from custom_loggers import *
from config import *

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# Function to check if Header_ID exists in DynamoDB PII table
def check_header_ids(header_ids):
    try:
        header_ids = list(set(header_ids))  # Remove duplicates
        key_conditions = [{'Header_Id': {'S': header_id}} for header_id in header_ids]
        response = dynamodb.batch_get_item(
            RequestItems={
                PII_TABLE: {
                    'Keys': key_conditions
                }
            }
        )
        results = {header_id: 'no' for header_id in header_ids}  # Default all to 'no'
        for item in response.get('Responses', {}).get(PII_TABLE, []):
            header_id = item['Header_Id']['S']
            results[header_id] = 'yes'
        return results
    except Exception as e:
        logger.error(f"Exception encountered while checking header ids: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}



# Function to chunk Spark DataFrame into batches using generator
def chunk_header_ids(df, chunk_size=25):
    try:
        def chunk_partition(partition):
            partition = list(partition)
            for i in range(0, len(partition), chunk_size):
                yield partition[i:i + chunk_size]

        # Convert DataFrame to RDD and apply chunking function to each partition
        chunked_rdd = df.rdd.mapPartitions(chunk_partition)
        return chunked_rdd
    except Exception as e:
        logger.error(f"Exception occurred in chunk_header_ids method: {e}")

    


# Check Header_ID existence using concurrency
def process_in_batches(df):
    try:
        chunked_rdd = chunk_header_ids(df, chunk_size=25)
        header_ids_batches = chunked_rdd.collect()
        results = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(check_header_ids, [row['Header_Id'] for row in batch]) for batch in header_ids_batches]
            for future in futures:
                results.update(future.result())
        return results
    except Exception as e:
        logger.error(f"Exception encountered while processing in batches: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}

def track_header_ids(header_id_df, sc):
    try:
        if header_id_df:
            result = process_in_batches(header_id_df)
            result_rdd = sc.parallelize(result.items())
            result_df = result_rdd.toDF(['Header_Id', 'available'])
        return result_df
    except Exception as e:
        logger.error(f"Exception encountered while   processing header ids: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None