from threading import Lock
from config import *
from custom_loggers import *
from pii_config import *
from common_util_methods import *

from pyspark.sql.functions import to_date

from datetime import datetime
import pytz


# AWS clients initialization
dynamodb_resource = boto3.resource('dynamodb', config=Config(
    region_name=AWS_REGION,
    max_pool_connections=AWS_MAX_POOL_CONNECTIONS,
    retries={'max_attempts': AWS_MAX_ATTEMPTS, 'mode': 'standard'}
))
client = boto3.client('dynamodb', config=Config(
    region_name=AWS_REGION,
    max_pool_connections=AWS_MAX_POOL_CONNECTIONS,
    retries={'max_attempts': AWS_MAX_ATTEMPTS, 'mode': 'standard'}
))


logger, log_stream = setup_in_memory_logging()

try:
    mapping_tbl = dynamodb_resource.Table(MAPPING_TABLE)
except Exception as e:
    logger.error(f"Error while geting table reference")
    raise


# Locks
memoization_cache_lock = Lock()
existing_data_lock = Lock()

# Variables
memoization_cache = {}
total_records_inserted = 0

def batch_write(items, table):
    if not isinstance(items, list):
        logger.error("Items should be a list of dictionaries.")
        return False
    if isinstance(table, str):
        table = dynamodb_resource.Table(table)    
     
    global total_records_inserted
    try:

        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
            
            total_records_inserted+=len(items)   
    except Exception as e:
        logger.error(f"Error in batch write: {e}")



def batch_get_items(Header_Ids, table_name, client, batch_size=BATCH_SIZE):
    """
    Batch retrieves items from DynamoDB based on a list of header IDs.

    Args:
    - Header_Ids (list): List of header IDs to retrieve from DynamoDB.
    - table_name (str): Name of the DynamoDB table.
    - client (DynamoDB.Client): Boto3 DynamoDB client instance.
    - batch_size (int, optional): Size of each batch for concurrent requests. Defaults to BATCH_SIZE.

    Returns:
    - dict: A dictionary mapping header IDs to retrieved items from DynamoDB.

    Raises:
    - Exception: If there's any issue with DynamoDB client operations or thread execution.
    """
    existing_items = {}
    keys = [{'Header_Id': {'S': Header_Id}} for Header_Id in Header_Ids]
    key_chunks = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]
    
    try:
        with ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS) as executor:
            # Submit batch requests asynchronously
            futures = [executor.submit(client.batch_get_item, RequestItems={table_name: {'Keys': chunk}})
                       for chunk in key_chunks]
            
            # Iterate over completed futures
            for future in as_completed(futures):
                try:
                    response = future.result()
                    responses = response.get('Responses', {}).get(table_name, [])
                    
                    # Process each item in the response
                    for item in responses:
                        Header_Id = item['Header_Id']['S']
                        existing_items[Header_Id] = item
                except Exception as e:
                    # Log any individual future execution errors
                    logger.error(f"Failed to process batch get item response: {e}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
    except Exception as e:
        # Log thread pool executor or client operation errors
        logger.error(f"Failed to batch get items from DynamoDB: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return existing_items



def generate_new_custId(val: str) -> str:
    """
    Generates a new Customer_Id based on the input value.

    Args:
    - val (str): Input value to generate the Customer_Id.

    Returns:
    - str: Generated Customer_Id.

    Raises:
    - ETLProcessException: If an error occurs while generating the Customer_Id.
    """
    try:
        # Generate Customer_Id using MD5 hash of the input value
        hashed_value = hashlib.md5(val.encode("UTF-8")).hexdigest()
        new_custId = str(uuid.UUID(hex=hashed_value))
        return new_custId
    
    except Exception as e:
        error_message = f'Exception encountered while generating Customer_Id for {val}: {e}'
        logger.info(error_message)



def get_enriched_mapping_df(mapping_df, spark):
    """
    Enriches a given DataFrame (`mapping_df`) with additional columns and transformations.

    Args:
    - mapping_df (DataFrame): Input DataFrame containing mapping data.
    - spark (SparkSession): Spark session object.

    Returns:
    - DataFrame: Enriched DataFrame with transformed columns.

    Notes:
    - Assumes `encrypt_mobile` and `generate_new_custId` are defined and imported correctly.
    """
    try:
        # Cast Enriched_Order_Date to StringType if necessary
        mapping_df = mapping_df.withColumn("Enriched_Order_Date", col("Enriched_Order_Date").cast(StringType()))
        mapping_df = mapping_df.withColumn("Enriched_Order_Date", date_format(to_date("Enriched_Order_Date", "yyyy/MM/dd"), "yyyy-MM-dd")) 

        # Define UDFs for custom transformations
        generate_new_custId_partial = partial(generate_new_custId)
        udf_generate_new_custId = udf(generate_new_custId_partial, StringType())

        secret_key, iv = load_key()  # Load encryption keys
        encrypt_mobile_partial = partial(encrypt_mobile, secret_key=secret_key, iv=iv)
        encrypt_mobile_udf = udf(encrypt_mobile_partial, StringType())

        # Apply transformations to DataFrame columns
        mapping_df = mapping_df.withColumn('Encrypt_mobile_number', encrypt_mobile_udf(col('Mobile_Number')))
        mapping_df = mapping_df.withColumn('Customer_Id', udf_generate_new_custId(col('Encrypt_mobile_number')))

    

        # Select and alias desired columns for the final DataFrame
        mapping_df = mapping_df.select(
            col('Header_Id').alias("Header_Id"),
            col('Customer_Id').alias("Customer_Id"),
            col('PHDV_Aggregator_Id').alias("Aggregator_Id"),
            col('Store_No').alias("Store_No"),
            col('Channel').alias('Channel'),
            col('Source').alias('Source'),
            col('Fulfilment_Mode').alias('Fullfillment'),
            col('Enriched_WebOrderNo').alias("Online_ref_ID"),
            col('PHDV_Reference_No').alias("Reference_No"),
            col('Receipt_No').alias("Receipt_No"),
            col('Transaction_No').alias("Transaction_No"),
            col('Invoice_No').alias("Invoice_No"),
            col('Enriched_Order_Date').alias("Enriched_Order_Date"),
            col('Enriched_Order_Time').alias("Enriched_Order_Time"),
            col('Franchise').alias("Franchise"),
            col('PHDV_Pizzahut_Brand').alias("Pizzahut_Brand"))
        

        # Handle empty Aggregator_Id
        mapping_df = mapping_df.withColumn(
            'Aggregator_Id',
            when(col('Aggregator_Id').isNull(), "  ")   
            .when(trim(col('Aggregator_Id')) == '', "  ")   
            .otherwise(col('Aggregator_Id')) 
        )

        return mapping_df

    except Exception as e:
        # Log any exceptions and return None
        logger.error(f"Exception in get_enriched_mapping_df method: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def chunk_spark_data(df, spark, chunk_size=25):
    """
    Function to chunk a Spark DataFrame into batches.

    Args:
    - df (DataFrame): Input Spark DataFrame to be chunked.
    - spark (SparkSession): Spark session object.
    - chunk_size (int, optional): Size of each chunk/batch. Defaults to 25.

    Yields:
    - list: A chunk of rows from the DataFrame as a list.

    Raises:
    - Exception: If there's any issue during the chunking process.
    """
    try:
        # Zip DataFrame rows with index
        indexed_rdd = df.rdd.zipWithIndex()

        # Group rows by chunk index
        grouped_rdd = indexed_rdd.map(lambda x: (x[1] // chunk_size, x[0])).groupByKey()

        # Convert grouped RDD to chunks
        chunked_rdd = grouped_rdd.map(lambda x: list(x[1]))

        # Yield each chunk from local iterator
        for chunk in chunked_rdd.toLocalIterator():
            yield chunk

    except Exception as e:
        # Log and raise exception if chunking fails
        logger.error(f"Failed to chunk Spark DataFrame: {e}")
        raise


def memoize_existing_items(existing_items):
    """
    Memoizes existing items into a memoization cache.

    Args:
    - existing_items (dict): Dictionary containing existing items to be memoized.

    Raises:
    - Exception: If there's an issue during the memoization process.
    """
    try:
        for Header_Id, record in existing_items.items():
            try:
                # Update memoization cache for each record
                memoization_cache[Header_Id] = {column: record.get(column) for column in UPDATE_COLUMNS}
            except Exception as e:
                # Log errors updating individual records
                error_message = f"Error updating record for Header_Id {Header_Id}: {e}"
                logger.error(error_message)
    except Exception as e:
        # Log overall memoization cache update error
        error_message = f"Update memoization cache error: {e}"
        logger.error(error_message)

def get_memoized_cache_df(spark):
    """
    Retrieves memoized data from `memoization_cache` and creates a DataFrame using Spark.

    Args:
    - spark (SparkSession): Spark session object.

    Returns:
    - DataFrame: Memoized data as a Spark DataFrame.

    Raises:
    - Exception: If there's an issue during DataFrame creation.
    """
    try:
        # Convert memoization_cache to RDD and parallelize
        rdd = spark.sparkContext.parallelize(memoization_cache.items())
        
        # Map RDD to (Header_Id, *values) format
        rdd_converted = rdd.map(lambda x: (x[0], *x[1].values()))
        
        # Create DataFrame from RDD with a specified schema
        # Note: Uncomment and provide SPARK_SCHEMA if needed
        # schema = StructType([StructField(name, StringType(), True) for name, _ in SPARK_SCHEMA])
        memoized_df = spark.createDataFrame(rdd_converted, schema)
        
        # Log DataFrame columns information
        logger.info(f"Columns of memoized_df: {memoized_df.columns}")
        
        return memoized_df
    
    except Exception as e:
        # Log and raise exception if DataFrame creation fails
        error_message = f"Error in get_memoized_df method: {e}"
        logger.error(error_message)
        raise



def process_Header_Id_df(Header_Id_df, spark, table_name):
    """
    Process a DataFrame of Header_Ids, fetch existing items asynchronously, and memoize them.

    Args:
    - Header_Id_df (DataFrame): DataFrame containing Header_Ids to process.
    - spark (SparkSession): Spark session object.
    - table_name (str): Name of the table to fetch items from.

    Raises:
    - Exception: If there's an issue during the processing.
    """
    try:
        existing_items = {}

        # Use ThreadPoolExecutor to asynchronously fetch items for each chunk of Header_Ids
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(batch_get_items, [row['Header_Id'] for row in chunk], table_name, client)
                for chunk in chunk_spark_data(Header_Id_df, spark)
            ]

            # Iterate over completed futures to gather batch items
            for future in as_completed(futures):
                try:
                    batch_items = future.result()
                    if batch_items is not None:
                        existing_items.update(batch_items)
                except Exception as e:
                    # Log errors encountered during batch item retrieval
                    logger.error(f"Exception encountered in batch_get_items: {e}")

            # Memoize existing items if any were found
            if existing_items:
                memoize_existing_items(existing_items)
            else:
                # Log a warning if no existing records were found
                logger.warning("No existing records found")

    except Exception as e:
        # Log and raise exception if processing fails
        logger.error(f"Failed to process Header_Id DataFrame: {e}")
        raise


def memoize_new_items(record):
    """
    Memoizes a new customer record into `memoization_cache`.

    Args:
    - record (dict): Dictionary containing the customer record to memoize.

    Raises:
    - Exception: If there's an issue during the memoization process.
    """
    try:
        Header_Id = record["Header_Id"]

        # Initialize record values with placeholders for specified columns
        record_values = {column: record.get(column, " ") for column in UPDATE_COLUMNS}

        # Use memoization_cache_lock to ensure thread-safe access to memoization_cache
        with memoization_cache_lock:
            if Header_Id in memoization_cache:
                # Update existing record in cache
                memoization_cache[Header_Id].update(record_values)
            else:
                # Create new record in cache
                memoization_cache[Header_Id] = record_values

    except Exception as e:
        # Log error if any exception occurs during the memoization process
        error_message = f"Error occurred while processing customer record: {e}"
        logger.error(error_message)



def process_enriched_mapping_df(enriched_mapping_df, spark):
    """
    Process an enriched mapping DataFrame asynchronously, memoize new items, and insert into DB.

    Args:
    - enriched_mapping_df (DataFrame): Enriched mapping DataFrame to process.
    - spark (SparkSession): Spark session object.

    Raises:
    - Exception: If there's an issue during the processing.
    """
    try:
        # Use ThreadPoolExecutor to asynchronously process each row in chunks of the DataFrame
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(memoize_new_items, row.asDict())
                for chunk in chunk_spark_data(enriched_mapping_df, spark)
                for row in chunk
            ]

            # Iterate over completed futures
            for future in as_completed(futures):
                try:
                    # Wait for each future to complete
                    future.result()
                except Exception as e:
                    # Log any exceptions encountered during memoization
                    error_message = f"Error occurred while performing concurrent operation on process_record method: {e}"
                    logger.error(error_message)

        # Retrieve memoized DataFrame from cache
        memoized_df = get_memoized_cache_df(spark)

        # Insert memoized records into the database
        if memoized_df:
            insert_records_into_db(memoized_df, spark)
        else:
            # Log a warning if memoized DataFrame is not available
            logger.warning("Memoized DataFrame is not available")
            return  # Return from the function if memoized_df is not available

    except Exception as e:
        # Log and raise exception if ThreadPoolExecutor execution fails
        error_message = f"Error occurred while executing ThreadPoolExecutor: {e}"
        logger.error(error_message)
        raise


def chunk_data(dict_list: List[Dict], chunk_size: int = CHUNK_SIZE) -> Generator[List[Dict], None, None]:
    """
    Chunk a list of dictionaries into smaller chunks.

    Args:
    - dict_list (List[Dict]): List of dictionaries to be chunked.
    - chunk_size (int, optional): Size of each chunk. Defaults to CHUNK_SIZE.

    Yields:
    - Generator[List[Dict], None, None]: Generator yielding chunks of dictionaries.

    Raises:
    - ValueError: If chunk_size is not positive.

    Example:
    - If dict_list = [{'a': 1}, {'b': 2}, {'c': 3}, {'d': 4}, {'e': 5}]
    - and chunk_size = 2,
    - the generator will yield:
      - [{'a': 1}, {'b': 2}]
      - [{'c': 3}, {'d': 4}]
      - [{'e': 5}]

    """
    try:
        if chunk_size <= 0:
            raise ValueError("Chunk size must be a positive integer.")

        for i in range(0, len(dict_list), chunk_size):
            yield dict_list[i:i + chunk_size]

    except ValueError as ve:
        # Log the error and raise it further
        logger.error(f"Error in chunk_data: {ve}")
        raise

    except Exception as e:
        # Log any unexpected errors and raise them further
        logger.error(f"Unexpected error in chunk_data: {e}")
        raise



def insert_records_into_db(memoized_df, spark):
    """
    Insert records from memoized DataFrame into a database using batch writing.

    Args:
    - memoized_df (DataFrame): Memoized DataFrame containing records to insert.
    - spark (SparkSession): Spark session object.

    Returns:
    - str: "OK" if insertion is successful.

    Raises:
    - Exception: If there's an issue during the insertion process.
    """
    try:
        memoized_df = memoized_df.na.fill('')
        dict_list=memoized_df.toPandas().to_dict(orient='records')
        logger.info(f"Total records to insert into Mapping table: {len(dict_list)}")
        

        # Use ThreadPoolExecutor to asynchronously process each chunk of DataFrame rows
        with ThreadPoolExecutor(max_workers=20) as executor:
            # futures = [
            #     executor.submit(batch_write, [row.asDict() for row in chunk], mapping_tbl)
            #     for chunk in chunk_spark_data(memoized_df, spark)
            # ]

            futures = [executor.submit(batch_write, chunk, mapping_tbl) for chunk in chunk_data(dict_list)]

            # Iterate over completed futures
            for future in as_completed(futures):
                try:
                    future.result()
                    
                except Exception as e:
                    # Log any exceptions encountered during batch writing
                    logger.error(f"Error occurred during batch writing: {e}")

        # Log total number of records successfully inserted
        logger.info(f"Total records inserted into mapping table: {total_records_inserted}")

        try:
            # Define the Indian timezone
            india_timezone = pytz.timezone('Asia/Kolkata')
                
            # Get the current date and time in Indian timezone
            current_time = datetime.now(india_timezone)
                
            # Format the date in yyyy/mm/dd format
            formatted_date = current_time.strftime('%Y/%m/%d')
                
            path=f"s3://ph-etl-archive/DIL_TEST_PROD_DATA_NORMALISATION_DATA/Dynamodb_Data_Dump/Mapping/{formatted_date}"

            # Save to S3
            memoized_df.repartition(1).write.mode("overwrite").parquet(path)
        except Exception as e:
            error_msg = f"Error in saving mapping data to {path}: {e}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise ValueError(f"Error: {e}")      


        return "OK"

    except Exception as e:
        # Log and raise exception if insertion fails
        error_message = f"Error occurred while inserting records into db: {e}"
        logger.error(error_message)
        raise



def process_mapping_data(mapping_df, spark):
    """
    Process mapping data by enriching, extracting Header_Ids, and processing asynchronously.

    Args:
    - mapping_df (DataFrame): Input mapping DataFrame to process.
    - spark (SparkSession): Spark session object.

    Raises:
    - Exception: If there's an issue during the processing.
    """
    try:

        # Check if mapping_df is None
        if mapping_df is None:
            logger.warning("No mapping data provided.")
            return
        
        # Log total number of records in the input mapping_df
        logger.info(f"Total records in mapping_df: {mapping_df.count()}")

        
        # Enrich mapping_df to get enriched_mapping_df
        enriched_mapping_df = get_enriched_mapping_df(mapping_df, spark)
        
        # Check if enriched_mapping_df is None
        if enriched_mapping_df is None:
            logger.warning("No enriched mapping data returned.")
            return
        
        # Log total number of records in enriched_mapping_df
        logger.info(f"Total records in enriched_mapping_df: {enriched_mapping_df.count()}")
        
        # Extract unique Header_Ids from enriched_mapping_df
        Header_Id_df = enriched_mapping_df.select("Header_Id").dropDuplicates()
        
        # Process Header_Ids if Header_Id_df is not None
        if Header_Id_df is not None:
            process_Header_Id_df(Header_Id_df, spark, mapping_tbl.table_name)
            process_enriched_mapping_df(enriched_mapping_df, spark)
        else:
            logger.warning("No Header_Id_df exists in process_mapping_data method")
            return
        
    except Exception as e:
        # Log and raise exception if any error occurs during the processing
        logger.error(f"Error in process_mapping_data: {e}")
        raise
