from pii_config import *
from sns_config import *
from common_util_methods import *

from datetime import datetime
import pytz


# Global Variables
memoization_cache = {}
# existing_records = {}
total_records_inserted = 0

def batch_write(items, table):
    global total_records_inserted
    if not isinstance(items, list):
        logger.error("Items should be a list of dictionaries.")
        return False
    if isinstance(table, str):
        table = dynamodb_resource.Table(table)    
    # logger.info(f"Batch writing items in table : {table}")    

    try:
        # logger.info(f"Items to write in {table} : {len(items)}")
        # logger.info(f"sample : {items[0]}")
        
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
            # logger.info(f"Inserted items in  {table} : {len(items)}") 
            total_records_inserted+=len(items)   
    except Exception as e:
        logger.error(f"Error in batch write: {e}")

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
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise ValueError(e)
    


def generate_batches(dataframe: DataFrame, spark, batch_size: int) -> Generator:
    """
    Generator function to yield batches of records from a DataFrame.

    Args:
    - dataframe (DataFrame): Input DataFrame containing records.
    - spark (SparkSession): SparkSession object.
    - batch_size (int): Size of each batch.

    Yields:
    - list: List of dictionaries where each dictionary represents a row from the DataFrame.

    Raises:
    - Exception: If any error occurs during batch generation.
    """
    try:
        data_iter = dataframe.toLocalIterator()  # Local iterator for DataFrame
        batch = []  # Initialize an empty batch list

        for row in data_iter:
            batch.append(row.asDict())  # Convert Row to dictionary and add to batch list
            if len(batch) == batch_size:
                yield batch  # Yield the current batch
                batch = []  # Reset batch

        if batch:
            yield batch  # Yield any remaining records in the final batch
    except Exception as e:
        error_message = f"Error generating batches: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise ValueError(e)    

   


######################################################################################################################################    


def get_customer_record_from_db(row: Row, existing_records: Dict[str, Dict[str, str]]) -> None:
    """
    Fetch customer records from DynamoDB based on the provided row's encrypted mobile number.

    Args:
    - row (Row): Row object containing 'Encrypted_Mobile_Number' field.
    - existing_records (Dict[str, Dict[str, str]]): Dictionary to store fetched customer records.

    Returns:
    - None

    Raises:
    - ValueError: If any error occurs during the database query.
    """
    mobile_number = row['Encrypted_Mobile_Number']

    try:
        # Log the start of the operation
        # logger.info(f"Fetching customer record for mobile number: {mobile_number}")

        # Query DynamoDB for customer records based on encrypted mobile number
        table=dynamodb_resource.Table(CUSTOMER_TABLE)

        response = table.query(
            IndexName='MobileNumberIndex',
            KeyConditionExpression=Key('MobileNumber').eq(mobile_number)
        )

        # Store fetched customer records with minimal lock usage
        items = response.get('Items', [])
        if items:
            with existing_records_lock:
                for item in items:
                    existing_records[mobile_number] = item
        

    except Exception as e:
        error_message = f"Error fetching customer record for {mobile_number}: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise ValueError(error_message)
        
def extract_existing_records(customer_df: DataFrame, spark) -> Dict[str, Dict[str, str]]:
    """
    Extract existing customer records from DynamoDB based on 'Encrypted_Mobile_Number' in customer_df.

    Args:
    - customer_df (DataFrame): DataFrame containing customer data.
    - spark (SparkSession): Spark session object.

    Returns:
    - existing_records (Dict[str, Dict[str, str]]): Dictionary containing fetched customer records.

    Raises:
    - Exception: If any error occurs during the extraction process.

    """
    existing_records = {}

    try:
        # Select unique 'Encrypted_Mobile_Number' from customer_df
        cleaned_phone_numbers_df: DataFrame = customer_df.select("Encrypted_Mobile_Number").dropDuplicates()

        # Use ThreadPoolExecutor for concurrent fetching of customer records
        with ThreadPoolExecutor() as executor:
            futures: List[ThreadPoolExecutor] = [executor.submit(get_customer_record_from_db, row, existing_records) for row in cleaned_phone_numbers_df.collect()]

            # Iterate over completed futures
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    error_message = f"Error in concurrent process while extracting existing records: {e}"
                    logger.error(error_message)
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    # send_sns_notification(error_message, "Extract Existing Records Error")

    except Exception as e:
        # Log and raise exception if any error occurs during extraction
        error_message = f"Error extracting existing records: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

    return existing_records

########################################################################################################################################

def update_memoization_cache(existing_records) :
    """
    Update memoization cache with customer records from existing_records dictionary.

    Returns:
    - None

    Raises:
    - Exception: If any error occurs during the update process.

    """
    try:
        global memoization_cache

        with memoization_cache_lock:
            for mobile_number, record in existing_records.items():
                try:
                    # Update memoization_cache with relevant fields from record
                    memoization_cache[mobile_number] = {
                        'Customer_Id': record["Customer_Id"],
                        'First_Order_Date': datetime.strptime(record["First_Order_Date"], "%Y-%m-%d").date(),
                        'Last_Order_Date': datetime.strptime(record["Last_Order_Date"], "%Y-%m-%d").date(),
                        'Valid': record["Valid"],
                        'Unsubscribed': record["Unsubscribed"],
                        'Unsubscribe_Date': (datetime.strptime(record["Unsubscribe_Date"], "%Y-%m-%d").date() if record["Unsubscribe_Date"] else None)
                    }
                except Exception as e:
                    error_message = f"Error updating record for {mobile_number}: {e}"
                    logger.error(error_message)
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    # Uncomment the line below if you have a function to send SNS notifications
                    # send_sns_notification(error_message, "Memoization Cache Update Error")
    except Exception as e:
        error_message = f"Update memoization cache error: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise ValueError(e)
    finally:
        del existing_records
        gc.collect()
        # log_available_memory()
    




    
    
    
def get_memoized_df(spark: SparkSession) -> DataFrame:
    """
    Fetches memoized data from memoization cache and converts it to a DataFrame.

    Args:
    - spark (SparkSession): Spark session object.

    Returns:
    - DataFrame: Memoized data converted to Spark DataFrame.

    Raises:
    - Exception: If any error occurs during the process.
    """
    try:
        global memoization_cache

        # Convert to a Pandas DataFrame
        pandas_df = pd.DataFrame.from_dict(memoization_cache, orient='index')
        
        # Reset index to turn the keys into a column
        pandas_df.reset_index(inplace=True)
        
        # Rename the columns
        pandas_df.rename(columns={'index': 'Mobile_Number'}, inplace=True)

        logger.info(f"pandas_df schema: {pandas_df.dtypes}")
        
        # Convert date columns to the desired format
        if 'First_Order_Date' in pandas_df.columns:
            pandas_df['First_Order_Date'] = pd.to_datetime(pandas_df['First_Order_Date']).dt.strftime('%Y-%m-%d')
        
        if 'Last_Order_Date' in pandas_df.columns:
            pandas_df['Last_Order_Date'] = pd.to_datetime(pandas_df['Last_Order_Date']).dt.strftime('%Y-%m-%d')

        if 'Unsubscribe_Date' in pandas_df.columns:
            # Convert to datetime, handle empty values by using 'coerce' to set them as NaT (Not a Time)
            pandas_df['Unsubscribe_Date'] = pd.to_datetime(pandas_df['Unsubscribe_Date'], errors='coerce')
            
            # Fill NaT values with an empty string or any other default value you prefer
            pandas_df['Unsubscribe_Date'] = pandas_df['Unsubscribe_Date'].dt.strftime('%Y-%m-%d').fillna('')

        
        memoized_df = spark.createDataFrame(pandas_df)
        # logger.info(f"memoized_df schema: {memoized_df.schema}")
        # logger.info(f"memoized_df count: {memoized_df.count()}")
        # logger.info(f"memoized_df : {memoized_df.head(5)}")
        
        # pandas_df=None
        
        return memoized_df

    except Exception as e:
        error_message = f"Error in get_memoized_df method: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise ValueError(e)
    finally:
        del pandas_df
        memoization_cache=None
        gc.collect()
        # log_available_memory()
        
        
    
        

    
def process_record(record: Dict[str, Union[str, bool, datetime]]) -> None:
    """
    Processes a single customer record and updates the memoization cache.

    Args:
    - record (Dict[str, Union[str, bool, datetime]]): Dictionary containing customer record data.
      Expected keys: 'Encrypted_Mobile_Number' (str), 'Valid' (bool), 'Order_Date' (datetime).

    Returns:
    - None

    Raises:
    - Exception: If any error occurs during record processing.
    """
    try:
        global memoization_cache

        customer_id=record["Customer_Id"]
        mobile_number = record["Encrypted_Mobile_Number"]
        valid = record["Valid"]
        order_date = record["Order_Date"]
        unsubscribed=record["Unsubscribed"]
        unsubscibe_date=record["Unsubscribe_Date"]

        # Make sure order_date is a datetime object
        # if isinstance(order_date, str):
        #     order_date = datetime.strptime(order_date, "%Y-%m-%d").date()

        with memoization_lock:
            if mobile_number in memoization_cache:
                cached_record = memoization_cache[mobile_number]
                # Update first and last order dates
                cached_record['First_Order_Date'] = min(cached_record['First_Order_Date'], order_date)
                cached_record['Last_Order_Date'] = max(cached_record['Last_Order_Date'], order_date)
                # Update validity flag if any record is valid
                cached_record['Valid'] = valid
                cached_record["Unsubscribed"]=unsubscribed
                cached_record["Unsubscribe_Date"]=unsubscibe_date
            else:
                # Create new record in cache
                memoization_cache[mobile_number] = {
                    'Customer_Id': customer_id,
                    'First_Order_Date': order_date,
                    'Last_Order_Date': order_date,
                    'Valid': valid,
                    'Unsubscribed':unsubscribed,
                    'Unsubscribe_Date':unsubscibe_date
                }
    except Exception as e:
        error_message = f"Error occurred while processing customer record: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Uncomment the line below if you have a function to send SNS notifications
        # send_sns_notification(error_message, "Customer Record Processing Error")
        raise ValueError(e)



########################################################################################################################################
def insert_customer_records_into_db(memoized_df, spark):
    """
    Inserts customer records from memoized_df into a database using batch processing.

    Args:
    - memoized_df (DataFrame): DataFrame containing customer records.
    - spark (SparkSession): SparkSession object.

    Returns:
    - str: Indicates success ('OK') if insertion is successful.

    Raises:
    - Exception: If any error occurs during database insertion.
    """
    try:
        
        # Function to safely update the total records inserted
        global total_records_inserted
        

        # Using ThreadPoolExecutor for concurrent batch processing
        with ThreadPoolExecutor(max_workers=25) as executor:
            # Submitting tasks for batch_write function with batches from generate_batches
            futures = [executor.submit(batch_write, batch, CUSTOMER_TABLE) for batch in generate_batches(memoized_df, spark, batch_size=25)]
            
            # Iterate through completed futures
            for future in as_completed(futures):
                try:
                    future.result()  # Get the result to raise exceptions if any
                except Exception as e:
                    logger.error(f"Error occurred during batch writing: {e}")

        logger.info(f"Total records inserted into customer table : {total_records_inserted}")
        

    except Exception as e:
        error_message = f"Error occurred while inserting customer records into database: {e}"
        logger.error(error_message)
        # send_sns_notification(error_message, "Database Insertion Error")
        raise ValueError(e)


        
#########################################################################################################################################
        
def process_customer_records(customer_df, spark):
    try:
        # logger.info(f"Size of customer df: {customer_df.count()} X {len(customer_df.columns)}")
        # logger.info(f"customer df schema: {customer_df.schema}")

        # Extract existing records from DynamoDB
        existing_records=extract_existing_records(customer_df, spark)

        # Populate memoization_cache with existing records
        if existing_records:
            update_memoization_cache(existing_records)

        # Process records concurrently
        # Convert Order_Date to date type
        customer_df = customer_df.withColumn("Order_Date", col("Order_Date").cast(DateType()))
        customer_df.cache()
        try:
            with ThreadPoolExecutor(max_workers=25) as executor:
                futures = [executor.submit(process_record, row.asDict()) for row in customer_df.collect()]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        error_message = f"Error occurred in concurrent operation on process_record method: {e}"
                        logger.error(error_message)
                        # send_sns_notification(error_message, "Concurrent Operation Error")
        except Exception as e:
            logger.error(f"Error processing customer records concurrently: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ValueError(e)
        finally:
            customer_df.unpersist()
            del customer_df
            gc.collect() 
            # log_available_memory()
            

        # Get memoized DataFrame
        memoized_df = get_memoized_df(spark)
        memoized_df=memoized_df.withColumn("Unsubscribe_Date", col("Unsubscribe_Date").cast("string"))
        memoized_df.cache()
        # Replace NaN values with empty strings
        # memoized_df = memoized_df.na.fill({"Unsubscribe_Date": ""})
        

        if memoized_df:
            logger.info(f"Memoized DataFrame schema: {memoized_df.schema}")
            logger.info(f"Memoized DataFrame records count: {memoized_df.count()}")
            # logger.info(f"Memoized DataFrame records sample: {memoized_df.head(2)}")

            # Insert memoized records into database
            try:
                insert_customer_records_into_db(memoized_df, spark)
            except Exception as e:
                error_message = f"Error in inserting records in customer table: {e}"
                logger.error(error_message)
                logger.error(f"Traceback: {traceback.format_exc()}")
                # send_sns_notification(error_message, "Customer Records Processing Error")
                raise ValueError(e)    
            try:
                # Define the Indian timezone
                india_timezone = pytz.timezone('Asia/Kolkata')
                
                # Get the current date and time in Indian timezone
                current_time = datetime.now(india_timezone)
                
                # Format the date in yyyy/mm/dd format
                formatted_date = current_time.strftime('%Y/%m/%d')
                
                path=f"s3://ph-etl-archive/DIL_TEST_PROD_DATA_NORMALISATION_DATA/Dynamodb_Data_Dump/Customer/{formatted_date}"
                # Load the Encryption key and IV
                secret_key,iv=load_key()

                decrypt_mobile_partial=partial(decrypt_mobile,secret_key=secret_key, iv=iv)
                decrypt_mobile_udf=udf(decrypt_mobile_partial, StringType())

                memoized_df = memoized_df.withColumn("Decrypted_Mobile_Number", decrypt_mobile_udf(memoized_df["Mobile_Number"]))

                logger.info(f"Memoized DataFrame schema to save on s3: {memoized_df.schema}")
                memoized_df.repartition(1).write.mode("overwrite").parquet(path)
            except Exception as e:
                error_message = f"Error in while saving records to {path}: {e}"
                logger.error(error_message)
                logger.error(f"Traceback: {traceback.format_exc()}")
                # send_sns_notification(error_message, "Customer Records Processing Error")
                raise ValueError(e)     



    except Exception as e:
        error_message = f"Error in process_customer_records method: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        # send_sns_notification(error_message, "Customer Records Processing Error")
        raise ValueError(e)
    finally:
        memoized_df.unpersist()
        del memoized_df
        gc.collect()
        # log_available_memory()
         
