
from pii_config import *
from sns_config import *
from config import *
# from sapphire_config import *
from common_util_methods import *
from Customer_process_methods import *
from pyspark.sql.functions import to_date, date_format, col
import gc
from datetime import datetime
import pytz

total_records_inserted = 0


###########################################################################################################################################

def batch_write(items, table):
    if not isinstance(items, list):
        logger.error("Items should be a list of dictionaries.")
        return False
    if isinstance(table, str):
        table = dynamodb_resource.Table(table)    
     
    global total_records_inserted
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
        





def process_orderpiidata(pii_df, spark):
    try:
        logger.info(f"baseline_to_orderpiidata method is called")
        # logger.info(f"pii_df schema: {pii_df.schema}")
        # logger.info(f"pii_df records count: {pii_df.count()}")
        logger.info("***"*50)
        pii_df.cache()

        pii_df=pii_df.dropDuplicates(["Header_Id"])
        # logger.info(f"pii_df records count after dropping duplicates: {pii_df.count()}")

        for old_name, new_name in pii_column_renaming.items():
            if old_name in pii_df.columns:
                pii_df = pii_df.withColumnRenamed(old_name, new_name)

        # logger.info(f"pii_df schema after renaming: {pii_df.schema}") 
        pii_df = pii_df.na.fill('')
        dict_list=pii_df.toPandas().to_dict(orient='records')
        try:
            
            with ThreadPoolExecutor(max_workers=25) as executor:
                # Submitting batch_write tasks for each chunk of dict_list
                futures = [executor.submit(batch_write, chunk, PII_TABLE) for chunk in chunk_data(dict_list)]
                
                # Iterating through completed futures
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error in concurrent batch process: {e}")

            logger.info(f"Total records inserted into {PII_TABLE}: {total_records_inserted}")

        except Exception as e:
            error_msg = f"Error in concurrent batch write: {e}"
            logger.error(error_msg)
            # Uncomment the line below if you have a function to send SNS notifications
            # send_sns_notification(error_msg, "Concurrent Batch Write Error")
            raise

        try:
            # Define the Indian timezone
            india_timezone = pytz.timezone('Asia/Kolkata')
                
            # Get the current date and time in Indian timezone
            current_time = datetime.now(india_timezone)
                
            # Format the date in yyyy/mm/dd format
            formatted_date = current_time.strftime('%Y/%m/%d')
                
            path=f"s3://ph-etl-archive/DIL_TEST_PROD_DATA_NORMALISATION_DATA/Dynamodb_Data_Dump/PII/{formatted_date}"

            # Load the Encryption key and IV
            secret_key,iv=load_key()

            # Decrypt Mobile Number
            decrypt_mobile_partial=partial(decrypt_mobile,secret_key=secret_key, iv=iv)
            decrypt_mobile_udf=udf(decrypt_mobile_partial, StringType())

            pii_df = pii_df.withColumn("Decrypted_Mobile_Number", decrypt_mobile_udf(pii_df["Mobile_Number"]))
            # List of columns to decrypt
            columns_to_decrypt = ["Customer_Name", "Email", "Customer_Address", "Latitude", "Longitude", "City"]

            for column in columns_to_decrypt:
                decrypt_partial = partial(decrypt_mobile, secret_key=secret_key, iv=iv)
                decrypt_udf = udf(decrypt_partial, StringType())
                pii_df = pii_df.withColumn(f"decrypted_{column}", decrypt_udf(col(column)))
                   
            logger.info(f"Pii_df schema after decryption: {pii_df.schema}")

            # Save to S3
            pii_df.repartition(1).write.mode("overwrite").parquet(path)
        except Exception as e:
            error_msg = f"Error in saving pii df to {path}: {e}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise ValueError(f"Error: {e}")      

    except Exception as e:
        error_msg = f"Error in process_orderpiidata(): {e}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise ValueError(f"Error: {e}")
    finally:
        pii_df.unpersist()
        del pii_df
        gc.collect()
        # log_available_memory()




def get_enriched_df(df, spark):
    try:
        # Load encryption key and initialization vector
        secret_key, iv = load_key()

        # Encrypt mobile number
        encrypt_mobile_partial = partial(encrypt_mobile, secret_key=secret_key, iv=iv)
        encrypt_mobile_udf = udf(encrypt_mobile_partial, StringType())
        df = df.withColumn('Encrypted_Mobile_Number', encrypt_mobile_udf(col('Mobile_Number'))) 

        # Generate Customer Id
        udf_generate_new_custId = udf(generate_new_custId, StringType())
        spark.udf.register("generate_new_custId", udf_generate_new_custId)
        df = df.withColumn("Customer_Id", udf_generate_new_custId(col("Encrypted_Mobile_Number")))

        # List of columns to encrypt
        columns_to_encrypt = ["Customer_Name", "Email", "Address", "Latitude", "Longitude", "City"]

        # Encrypt specified columns
        for column in columns_to_encrypt:
            encrypt_partial = partial(encrypt_mobile, secret_key=secret_key, iv=iv)
            encrypt_udf = udf(encrypt_partial, StringType())
            df = df.withColumn(column, encrypt_udf(col(column)))

        df=df.drop("Mobile_Number")    

    except Exception as e:
        logger.error(f"Exception occurred in get_enriched_df() : {e}")
        raise ValueError(e)

    return df


######################################################################################################################################

def process_pii_records(baseline_df,mapping_dict, spark):
    """
    Process PII records from the baseline DataFrame, perform necessary transformations,
    and load into DynamoDB.

    Args:
    - baseline_df (DataFrame): DataFrame containing baseline data.
    - spark (SparkSession): Spark session object.

    Returns:
    - None

    Raises:
    - Exception: If any error occurs during processing.

    """
    try:
        # Log baseline DataFrame information
        logger.info(f'PII DataFrame Count: {baseline_df.count()}\n PII Data Columns: "{baseline_df.columns}"')
        # log_available_memory()

        for old_name, new_name in mapping_dict.items():
            if old_name in baseline_df.columns:
                baseline_df = baseline_df.withColumnRenamed(old_name, new_name)

        # logger.info("***"*10)
        # logger.info(f"df sample: {baseline_df.head(2)}")

        baseline_df = baseline_df.withColumn("Order_Date", 
                   date_format(to_date(col("Order_Date"), "yyyy/MM/dd"), "yyyy-MM-dd").cast("string"))
        
        # Add a new boolean column 'Unsubscribed' with a default value False
        baseline_df = baseline_df.withColumn("Unsubscribed", lit(False))

        # Add a new string column 'Unsubscribe_Date' without a default value (as null)
        baseline_df = baseline_df.withColumn("Unsubscribe_Date", lit("").cast("string"))

        
        # logger.info(f"df sample after Enriched_Order_Date format updation: {baseline_df.head(2)}")

        
        # Encrypt required fields
        df=get_enriched_df(baseline_df,spark)

        try:
            baseline_df=None
            # logger.info("baseline_df is set to None")
            # log_available_memory()
        except Exception as e:
            pass
        finally:
            gc.collect()    
        

        logger.info(f"***"*50)
        # logger.info(f"dataframe schema after enrichment: {df.schema}")
        logger.info(f"dataframe records count after enrichment: {df.count()}")
        # logger.info(f"dataframe records sample after enrichment: {df.head(2)}")
        
        # Select relevant columns from baseline DataFrame
        contact_df = df.select(*SELECTED_COLUMNS)
        try:
            logger.info(f"contact_df schema : {contact_df.schema}")
            logger.info(f"contact_df records count : {contact_df.count()}")
            # logger.info(f"contact_df records : {contact_df.head(2)}")

            # Process customer records to obtain customer DataFrame
            process_customer_records(contact_df, spark)
        except Exception as e:
            logger.error(f"Exception occurred while processing customer records in Customer table")  
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ValueError(e)  
        finally:
            contact_df=None
            logger.info("contact_df is set to None")
            # log_available_memory()    

        try:
                # Rename column
                df = df.withColumnRenamed("Encrypted_Mobile_Number", "Mobile_Number")

                # Select PII_COLUMNS from df
                pii_df = df.select(*PII_COLUMNS)
                logger.info(f"***"*50)
                # logger.info(f"pii_df schema after enrichment: {pii_df.schema}")
                # logger.info(f"pii_df records count after enrichment: {pii_df.count()}")
                # logger.info(f"pii_df records sample after enrichment: {pii_df.head(2)}")
                

                # Process PII data and insert into DynamoDB
                process_orderpiidata(pii_df, spark)
        except NameError as e:
            
            logger.error(f"Error while processing records in PII table : {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ValueError(e)
        


    except Exception as e:
        # Log general error during processing
        error_message = f"Error in process_pii_records function: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise ValueError(e)

