from config import *
from data_loading_functions import *
from payment_df_processing import *
# from new_phdv import *
from new_phdv_live import *
from phone_number_processing import *
from items_df_processing import *
from sns_config import *
# from channel_mapping_config import *
from Channel_Mapping_updated import *
from save_dataframe_script import *
import warnings
import psutil
from pyspark.sql.functions import upper

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor, as_completed
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


from common_util_methods import *
from PII_process_methods_updated import *
from Customer_process_methods import *
from track_header_ids import *
from pii_config import *
from get_final_df import *
from mapping_data_process_updated import *
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import StorageLevel
from awsglue.context import GlueContext
from awsglue.job import Job
import gc

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("dil_data_normalisation") \
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()


# Create SparkContext from the SparkSession
sc = spark.sparkContext

# Initialize GlueContext with SparkContext
glueContext = GlueContext(sc)

# Get the SparkSession from GlueContext
spark = glueContext.spark_session
job = Job(glueContext)

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']
# vcpu = get_max_vcpus(job_name)
warnings.filterwarnings("ignore", category=FutureWarning)

job.init(args['JOB_NAME'], args)
logger.info("Statted ETL Process for DIL")

# Record the start time of the ETL job
import time
etl_start_time = time.time()

def log_dataframe_size(df, name):
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    size_in_mb = size_in_bytes / (1024 * 1024)
    size_in_gb = size_in_bytes / (1024 * 1024 * 1024)
    
    # Log the size in MB and GB
    logger.info(f"Size in MB {name}: {size_in_mb:.2f} MB")
    logger.info(f"Size in GB {name}: {size_in_gb:.2f} GB")
    # logger.info(df.explain(mode='cost'))

    
def log_available_memory():
    memory_info = psutil.virtual_memory()
    # Available memory in bytes
    available_memory = memory_info.available
    
    # Convert to MB for readability
    available_memory_mb = available_memory / (1024 * 1024)

    logger.info(f"Available Memory: {available_memory_mb:.2f} MB")


def filter_mandatory_columns(header_df,header_df_mandatory_data_columns, data_save_s3_path, output_folder):
    """
    Filter the DataFrame to identify and handle rows with missing values in mandatory columns.
    
    Parameters:
    - header_df: DataFrame containing the header data.
    - header_df_mandatory_data_columns: List of columns that are mandatory.
    - data_save_s3_path: S3 path for saving DataFrames with missing values.
    
    Returns:
    - mandatory_missing_values_df: DataFrame with rows that have missing values in mandatory columns.
    - header_df: Cleaned DataFrame without rows having missing values in mandatory columns.
    """
    try:
        # Validate input DataFrame
        if header_df is None or not isinstance(header_df, DataFrame):
            raise ValueError("Input header_df is not a valid DataFrame in filter_mandatory_columns() ")
        
        header_df_count=header_df.count()
        
        # Create a filter condition to check for missing values in any of the specified columns
        filter_condition = col(header_df_mandatory_data_columns[0]).isNull()
        for column in header_df_mandatory_data_columns[1:]:
            filter_condition |= col(column).isNull()
        
        # Create a new DataFrame with rows where any specified column has missing values
        mandatory_missing_values_df = header_df.filter(filter_condition)
        
        if not mandatory_missing_values_df.isEmpty():
            logger.info(f"mandatory_missing_values_df schema: {mandatory_missing_values_df.schema}")
            # Save the DataFrame with missing values
            # save_dataframe(mandatory_missing_values_df, data_save_s3_path, "MANDATORY_COLUMNS_DATA_MISSING", 'InvoiceDate')
            mandatory_missing_values_df.repartition(1).write.option('header', True).mode("append").parquet(f"{data_save_s3_path}MANDATORY_MISSING_DATA/{output_folder}")
            mandatory_missing_values_df_count = mandatory_missing_values_df.count()
            
            # Create a new DataFrame without rows that have missing values in the specified columns
            header_df = header_df.filter(~filter_condition)
            logger.info(f"Header dataFrame Count after Mandatory columns Missing Data Remove: {header_df_count - mandatory_missing_values_df_count}")
            logger.info(f"Mandatory Columns Missing data: {mandatory_missing_values_df_count}")
            logger.info(f"Saved mandatory_missing_values_df to {data_save_s3_path}MANDATORY_COLUMNS_DATA_MISSING/{output_folder}")  
            
            if header_df is None or not isinstance(header_df, DataFrame):
                raise ValueError("Filtered header_df is not a valid DataFrame in filter_mandatory_columns()")
            return header_df
        else:
            logger.warning("mandatory_missing_values_df is empty in filter_mandatory_columns()")
            return header_df    
    except KeyError as e:
        error_msg = f"KeyError occurred: {e}. Check if mandatory columns exist in the DataFrame."
        subject = f"{job_name} - KeyError in filter_mandatory_columns()"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise KeyError(error_msg)
        
    except Exception as e:
        error_msg = f"An unexpected error occurred while filtering mandatory columns: {e}"
        subject = f"{job_name} - {type(e).__name__} in filter_mandatory_columns()"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise ValueError(error_msg)
    finally:
        mandatory_missing_values_df.unpersist()
        del mandatory_missing_values_df
        
def process_invoice_dates(header_df):
    """
    Process the 'InvoiceDate' column in the DataFrame and create a 'Header_Id'.
    
    Parameters:
    - header_df: DataFrame containing the invoice data.
    
    Returns:
    - header_df: DataFrame with formatted 'InvoiceDate' and new 'Header_Id' column.
    """
    try:
        # Validate input DataFrame
        if header_df is None or not isinstance(header_df, DataFrame):
            raise ValueError("Input header_df is not a valid DataFrame in process_invoice_dates()")
        
        # Convert the date column to the desired format
        # invoice_date_before = header_df.select('InvoiceDate').distinct().distinct().rdd.map(lambda row: row['InvoiceDate']).collect()
        logger.info("Adding new Format of InvoiceDate and creating Header_Id")
        
        header_df = header_df.withColumn("InvoiceDate", convert_date_format_udf(header_df['InvoiceDate']))
        invoice_date_distnct_df = header_df.select('InvoiceDate').distinct()
        distinct_dates_list = [row['InvoiceDate'] for row in invoice_date_distnct_df.collect()]
        logger.info(f"Data available for {distinct_dates_list}")
        
        # header_df = header_df.withColumn("InvoiceDate", date_format(to_date(col("InvoiceDate"), "MM/dd/yy"), "yyyy/MM/dd"))
        # invoice_date_after = header_df.select('InvoiceDate').distinct().distinct().rdd.map(lambda row: row['InvoiceDate']).collect()
        
        # Create Header_Id column by removing slashes from InvoiceDate and concatenating with OrderId and StoreCode
        header_df = header_df.withColumn("Header_Id", concat_ws("_", "OrderId", regexp_replace("InvoiceDate", "/", ""), "StoreCode"))
        header_df = header_df.withColumn("Orderwise_Id", concat_ws("_", "TransactionNo","PosTerminalNo", "StoreCode", regexp_replace("InvoiceDate", "/", "")))
        
        # logger.info(f"InvoiceDate Before: {invoice_date_before}\nInvoiceDate After:{invoice_date_after}")
        if header_df is None or not isinstance(header_df, DataFrame):
            raise ValueError("header_df is not a valid DataFrame in process_invoice_dates()")
        return header_df
    
    except KeyError as e:
        error_msg = f"KeyError occurred while processing invoice dates: {e}. Check if required columns exist in the DataFrame."
        subject = f"{job_name} - KeyError in process_invoice_dates()"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise KeyError(error_msg)
        
    except Exception as e:
        error_msg = f"An unexpected error occurred while processing invoice dates: {e}"
        subject = f"{job_name} - {type(e).__name__} in process_invoice_dates()"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise RuntimeError(error_msg)


def track_already_processed_data(header_df,header_df_count):
    """
    Track already processed header IDs to filter out records that have been processed.
    
    Parameters:
    - header_df: DataFrame containing header data with 'Header_Id'.
    
    Returns:
    - header_df: DataFrame filtered to exclude already processed records.
    """
    try:
        # Validate input DataFrame
        if header_df is None or not isinstance(header_df, DataFrame):
            raise ValueError("Input header_df is not a valid DataFrame in track_already_processed_data() ")
    
        if 'Header_Id' not in header_df.columns:
            raise KeyError("The required column 'Header_Id' is missing from the DataFrame in track_already_processed_data() ")
            
        # Tracking of Already Processed DataFrame
        logger.info("Checking for Already Processed Data..!")
        df = track_header_ids(header_df.select('Header_Id'), sc)
        df = df.withColumnRenamed('Header_Id', 'already_Processed_Header_Id')
        
        # Perform join on different column names
        result_df = header_df.join(df, header_df.Header_Id == df.already_Processed_Header_Id, how="left")
        logger.info(f"{result_df.count()}")
        
        if result_df:
            header_df = result_df.filter(col('available') == "no")
            header_df = header_df.dropDuplicates()
            already_Processed_Header_Id = result_df.filter(col('available') == "yes")
            already_Processed_Header_Id_count = already_Processed_Header_Id.count()
            logger.info(f"Already Processed Data count: {already_Processed_Header_Id_count}")
            logger.info(f"Header df will be Processed: {header_df_count - already_Processed_Header_Id_count}")
        else:
            logger.info("No already_Processed_Header_Id")
        
        return header_df   
    
    except Exception as e:
        error_msg = f"Error in Header ID tracking: {e}"
        subject = f"{job_name} - {type(e).__name__} in track_already_processed_data()"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        # raise RuntimeError(error_msg)
        return None

def merge_final_dataframes(header_df_without_webOrder_number, phdv_merged_header_df, record_did_not_match):
    """
    Merge multiple DataFrames into a final header DataFrame while handling exceptions and resource management.

    Parameters:
    - header_df_without_webOrder_number: DataFrame without web order numbers.
    - phdv_merged_header_df: DataFrame containing PHDV merged headers.
    - record_did_not_match: DataFrame with records that did not match.

    Returns:
    - final_header_df: Merged DataFrame with unique records or None if an error occurs.
    """
    try:
        if not phdv_merged_header_df.isEmpty():
            if not header_df_without_webOrder_number.isEmpty():
                header_df_without_match_and_weborderno = header_df_without_webOrder_number.unionByName(record_did_not_match, allowMissingColumns=True)
                final_header_df = header_df_without_match_and_weborderno.unionByName(phdv_merged_header_df, allowMissingColumns=True)
                final_header_df = final_header_df.dropDuplicates(["Header_Id"])
                logger.info("====" * 40)
                logger.info(f"Final Header Dataframe Count after PHDV merge: {final_header_df.count()}")
                return final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
                
            elif header_df_without_webOrder_number.isEmpty():
                try:
                    logger.info("header_df_without_webOrder_number is Not Available")
                    final_header_df = phdv_merged_header_df.unionByName(record_did_not_match, allowMissingColumns=True)
                    final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
                    logger.info("====" * 40)
                    logger.info(f"Final Header Dataframe Count after PHDV merge: {final_header_df.count()}")
                    return final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
                except Exception as e:
                    error_msg = f"Error during union of phdv_merged_header_df and record_did_not_match: {e}"
                    subject = f"{job_name} - {type(e).__name__}  in union operation within merge_final_dataframes"
                    send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                    logger.error(error_msg)
                    logger.error(traceback.format_exc())
                    raise RuntimeError(error_msg)
                    
            elif record_did_not_match.isEmpty() and not header_df_without_webOrder_number.isEmpty() and not phdv_merged_header_df.isEmpty():
                try:
                    logger.info("header_df_without_webOrder_number is Not Available")
                    final_header_df = phdv_merged_header_df.unionByName(header_df_without_webOrder_number, allowMissingColumns=True)
                    final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
                    logger.info("====" * 40)
                    logger.info(f"Final Header Dataframe Count after PHDV merge: {final_header_df.count()}")
                    return final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
                except Exception as e:
                    error_msg = f"Error during union of phdv_merged_header_df and record_did_not_match: {e}"
                    subject = f"{job_name} - {type(e).__name__}  in union operation within merge_final_dataframes"
                    send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                    logger.error(error_msg)
                    logger.error(traceback.format_exc())
                    raise RuntimeError(error_msg)
            else:
                try:
                    logger.info("header_df_without_webOrder_number is Not Available")
                    final_header_df = phdv_merged_header_df.unionByName(record_did_not_match, allowMissingColumns=True)
                    final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
                    logger.info("====" * 40)
                    logger.info(f"Final Header Dataframe Count after PHDV merge: {final_header_df.count()}")
                    return final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
                except Exception as e:
                    error_msg = f"Error during union of phdv_merged_header_df and record_did_not_match: {e}"
                    subject = f"{job_name} - {type(e).__name__}  in union operation within merge_final_dataframes"
                    send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                    logger.error(error_msg)
                    logger.error(traceback.format_exc())
                    raise RuntimeError(error_msg)
                finally:
                    phdv_merged_header_df.unpersist()
                    record_did_not_match.unpersist()
                    del phdv_merged_header_df
                    del record_did_not_match
    except Exception as e:
        error_msg = f"Error in merging final DataFrames: {e}"
        subject = f"{job_name} - {type(e).__name__}  in merge_final_dataframes"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise RuntimeError(error_msg)
    finally:
        # Unpersist DataFrames to free up memory
        if 'header_df_without_match_and_weborderno' in locals():
            header_df_without_match_and_weborderno.unpersist()
        
        if record_did_not_match is not None:
            record_did_not_match.unpersist()
        
        if header_df_without_webOrder_number is not None:
            header_df_without_webOrder_number.unpersist()

        # Clean up references to DataFrames
        del header_df_without_webOrder_number
        del record_did_not_match
        if 'header_df_without_match_and_weborderno' in locals():
            del header_df_without_match_and_weborderno
        gc.collect()    

def process_web_order_numbers(header_df, data_save_s3_path):
    """
    Process DataFrame to separate web order numbers and perform further operations.

    Parameters:
    - header_df: DataFrame containing invoice data.
    - data_save_s3_path: S3 path for saving processed data.

    Returns:
    - final_header_df: Merged DataFrame or None if an error occurs.
    """
    try:
        if header_df is None or not isinstance(header_df, DataFrame):
            raise ValueError("Invalid header_df: must be a non-null DataFrame in process_web_order_numbers()")
        try:
            header_df_with_webOrder_number = header_df.filter(col('WeborderNo').isNotNull())
            header_df_with_webOrder_number.persist(StorageLevel.MEMORY_AND_DISK)
            header_df_with_webOrder_number = header_df_with_webOrder_number.withColumn("Enriched_WebOrder_No", get_enriched_webOrderNumber_udf(col("WeborderNo")))
            header_df_with_webOrder_number_count = header_df_with_webOrder_number.count()
            header_df_without_webOrder_number = header_df.filter(col('WeborderNo').isNull())
            header_df_without_webOrder_number.persist(StorageLevel.MEMORY_AND_DISK)
            header_df_without_webOrder_number_count = header_df_without_webOrder_number.count()
            logger.info(f'header_df_with_webOrder_number count: {header_df_with_webOrder_number_count}\nheader_df_without_webOrder_number count: {header_df_without_webOrder_number_count}')
        except (TypeError, ValueError) as e:
            error_msg = f"Type or Value Error in DataFrame separation on WeborderNumber: {e}"
            subject = f"{job_name} - Type or Value Error in DataFrame Separation"
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise RuntimeError(error_msg)
        except (AnalysisException, Py4JJavaError) as e:
            error_msg = f"Spark-specific error in DataFrame separation on WeborderNumber: {e}"
            subject = f"{job_name} - Spark Analysis Error in DataFrame Separation"
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Error in DataFrame separation on WeborderNumber: {e}"
            subject = f"{job_name} - {type(e).__name__}  in DataFrame Separation"
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise RuntimeError(error_msg)
        finally:
            header_df.unpersist()    
            del header_df
        
        try:
            # Select distinct InvoiceDate values
            invoice_date_df = header_df_with_webOrder_number.select('InvoiceDate').distinct()
            if invoice_date_df.isEmpty():
                logger.warning("No distinct InvoiceDate found.")
                raise ValueError("No distinct InvoiceDate found.")
        except (AnalysisException, Py4JJavaError) as e:
            error_msg = f'Error in generating Invoice Date list (Spark-specific): {e}'
            subject = f"{job_name} - Spark Error in Invoice Date Generation"
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f'Error in generating Invoice Date list: {e}'
            subject = f"{job_name} - {type(e).__name__}  in Invoice Date Generation"
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise RuntimeError(error_msg)
        
        if not invoice_date_df.isEmpty() and not header_df_with_webOrder_number.isEmpty():
            phdv_merged_header_df, record_did_not_match = get_phdv_merged_df(header_df_with_webOrder_number, invoice_date_df, PHDV_PROCESSING_DAYS, spark, 'dil', RAW_RAW_PHDV_SCHEMA)
            if not phdv_merged_header_df.isEmpty():
                final_header_df = merge_final_dataframes(header_df_without_webOrder_number, phdv_merged_header_df, record_did_not_match)
                return final_header_df
            else:
                logger.warning("phdv_merged_header_df is empty after merging.")
                return None
        
    except (AnalysisException, Py4JJavaError) as e:
        error_msg = f"Spark-specific error in processing web order numbers: {e}"
        subject = f"{job_name} - Spark Error in process_web_order_numbers"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise RuntimeError(error_msg)
    except Exception as e:
        error_msg = f"Error in processing web order numbers: {e}"
        subject = f"{job_name} - {type(e).__name__}  in process_web_order_numbers"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise RuntimeError(error_msg)
        
        

def process_payment_data(spark,final_header_df, payment_df_columns, payment_df_rename_mapping):
    """
    Process payment data from the final header DataFrame and join with gift card information if available.

    Parameters:
    - final_header_df: DataFrame containing the final header data.
    - payment_df_columns: List of columns to select from the final header DataFrame.
    - payment_df_rename_mapping: Mapping for renaming payment DataFrame columns.

    Returns:
    - payments_df: Processed payments DataFrame.
    - gift_card_info_df: DataFrame containing gift card information or None if an error occurs.
    """
    try:
        logger.info("Started Processing Payment Dataframe")
        payment_df_raw = final_header_df.select(*payment_df_columns)
        payments_df, gift_card_info_df = process_payment_df(payment_df_raw, payment_df_rename_mapping)
        logger.info('Finished processing payments_df')
        
        # if not gift_card_info_df.isEmpty():
        #     logger.info("Gift Card Info Found and Joining with Baseline DataFrame")
        #     final_header_df = final_header_df.join(gift_card_info_df, on="Header_Id", how='left')
        logger.info(f"payments_df Count: {payments_df.count()}")
        logger.info(f"gift_card_info_df Count: {gift_card_info_df.count()}")
        return payments_df, gift_card_info_df
    except Exception as e:
        error_msg = f"Error in Payment DataFrame Processing:"
        subject = f"{job_name} - {type(e).__name__} in Payment DataFrame Processing"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        # logger.error(error_msg)
        logger.error(traceback.format_exc())

def process_item_data(spark,final_header_df, item_df_columns, final_items_columns):
    """
    Process item data from the final header DataFrame.

    Parameters:
    - final_header_df: DataFrame containing the final header data.
    - item_df_columns: List of columns to select from the final header DataFrame for items.
    - final_items_columns: List of columns to be used in the processed items DataFrame.

    Returns:
    - items_df: Processed items DataFrame or None if an error occurs.
    """
    try:
        logger.info("Started Processing Line Dataframe")
        final_header_df = final_header_df.withColumn("Store_Franchise", col("StoreMaster_Franchise"))
        print("***"*30)
        print(f"{final_header_df.columns}")
        print("***"*30)
        items_df_raw = final_header_df.select(*item_df_columns)
        items_df = process_item_df(items_df_raw, final_items_columns)
        logger.info("Finished Processing Items DataFrame")
        return items_df
    except Exception as e:
        error_msg = f"Error in Items DataFrame Processing:"
        subject = f"{job_name} - {type(e).__name__} in Items DataFrame Processing"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        # logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise




        
def process_pii_data(final_header_df, final_header_df_rename_mapping, pii_df_columns, raw_to_pii_column_rename_mapping, spark):
    """
    Process PII data from the final header DataFrame and push it to DynamoDB.

    Parameters:
    - final_header_df: DataFrame containing the final header data.
    - final_header_df_rename_mapping: Mapping for renaming columns in the header DataFrame.
    - pii_df_columns: List of PII columns to select.
    - raw_to_pii_column_rename_mapping: Mapping for renaming raw columns to PII columns.
    - spark: SparkSession object for processing.

    Returns:
    - None
    """
    try:
        logger.info("Started Processing PII Dataframe to Database")
        pii_df = get_pii_df(final_header_df, final_header_df_rename_mapping, pii_df_columns)
        logger.info(f"Count Of PII DataFrame: {pii_df.count()}")
        if not pii_df.isEmpty():
            logger.info("Adding Address column as empty PII Dataframe")
            pii_data = pii_df.withColumn("Address", lit(None).cast(StringType()))
            pii_data.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                # logger.info(pii_data.schema)
                logger.info("Send PII Dataframe For Processing and Pushing to DynamoDb")
                
                pii_data=pii_data.withColumn('Franchise', upper(pii_data['Franchise']))
                pii_data = pii_data.na.fill({'Franchise': 'DIL'})
                
                process_pii_records(pii_data, raw_to_pii_column_rename_mapping, spark)
            except Exception as e:
                error_msg = f"Error in processing PII records: {e}"
                subject = f"{job_name} - {type(e).__name__}  in Processing PII Records"
                send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                logger.error(error_msg)
                logger.error(traceback.format_exc())
                raise RuntimeError(error_msg)
            finally:
                pii_data.unpersist()
                del pii_data
    except Exception as e:
        error_msg = f"Error in PII Data Processing: {e}"
        subject = f"{job_name} - {type(e).__name__} in PII Data Processing"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        gc.collect()
        
        

def process_mapping_records(final_header_df, final_header_df_rename_mapping, mapping_dataframe_columns, spark):
    """
    Process mapping data from the final header DataFrame and push it to the mapping database.

    Parameters:
    - final_header_df: DataFrame containing the final header data.
    - final_header_df_rename_mapping: Mapping for renaming columns in the header DataFrame.
    - mapping_dataframe_columns: List of columns to select for the mapping DataFrame.
    - spark: SparkSession object for processing.

    Returns:
    - None
    """
    mapping_df = None
    try:
        mapping_df = get_mapping_df(final_header_df, final_header_df_rename_mapping, mapping_dataframe_columns)
        mapping_df.persist(StorageLevel.MEMORY_AND_DISK)
        
        if mapping_df and not mapping_df.isEmpty():
            
            mapping_df=mapping_df.withColumn('Franchise', upper(mapping_df['Franchise']))
            mapping_df = mapping_df.na.fill({'Franchise': 'DIL'})
            
            # logger.info(f"mapping_df schema: {mapping_df.schema}")
            logger.info(f"Sent Data For Mapping Database: {mapping_df.count()}")
            process_mapping_data(mapping_df, spark)
    except Exception as e:
        logger.error(f"Error In Mapping Data Process: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        error_msg = f"Error in Mapping Data Process: {e}"
        subject = f"{job_name} - {type(e).__name__} in Mapping Data Process"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
    finally:
        if mapping_df:
            mapping_df.unpersist()
            del mapping_df
        gc.collect()




def WriteToPosgres(df,table_name,mode="append"):
    try:
        # Configure  JDBC connection
        df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres.cuczkjkpjxnq.ap-south-1.rds.amazonaws.com:5432/Data_Normalized_db") \
        .option("dbtable", table_name) \
        .option("user", "postgres_glue") \
        .option("password", "e0ODgWnbqx9ngISYPwdc") \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()
    except Exception as e:
        
        error_msg = f"Error during writing data to table: {table_name} - {e}"
        subject = f"{job_name} - {type(e).__name__} in WriteToPosgres()"
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error(error_msg)
        raise ValueError(f"Failed to write DataFrame to table '{table_name}': {str(e)}")    

##################################################################################################################################

def execute_etl_process(final_header_df, payment_df_columns, payment_df_rename_mapping,
                        item_df_columns, final_items_columns, data_save_s3_path,
                        output_folder, final_header_df_rename_mapping,
                        baseline_columns,input_data_folder,data_process_date, spark):
    
    """
    Execute the ETL process for the given DataFrames.

    Parameters:
    - final_header_df: DataFrame containing the final header data.
    - payment_df_columns: Columns to be used for payment DataFrame.
    - payment_df_rename_mapping: Mapping for renaming payment columns.
    - item_df_columns: Columns to be used for item DataFrame.
    - final_items_columns: Columns for final items DataFrame.
    - data_save_s3_path: S3 path to save processed DataFrames.
    - output_folder: Output folder for saving results.
    - final_header_df_rename_mapping: Mapping for renaming final header DataFrame columns.
    - baseline_columns: Columns to be included in baseline DataFrame.
    - spark: SparkSession object for processing.

    Returns:
    - None
    """                         
    try:
        # Step 1: Process DataFrames concurrently
        start_time=time.time()
        with ThreadPoolExecutor(max_workers=2) as executor:
            payment_future = executor.submit(process_payment_data,spark, final_header_df, payment_df_columns, payment_df_rename_mapping)
            item_future = executor.submit(process_item_data,spark, final_header_df, item_df_columns, final_items_columns)

            payments_df, gift_card_info_df = None, None
            items_df = None

            # Collect results from futures
            for future in as_completed([payment_future, item_future]):
                try:
                    if future == payment_future:
                        payments_df, gift_card_info_df = future.result()
                    elif future == item_future:
                        items_df = future.result()
                except Exception as e:
                    logger.error("Processing failed: %s", e)
                    subject = f"{job_name} - {type(e).__name__} in payment or item DataFrame Processing"
                    logger.error("Traceback: %s", traceback.format_exc())
                    send_sns_notification(f"Error in DataFrame Processing: {e}", subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                    raise

        logger.info(f"Time taken to process payments and item data: {(time.time()-start_time)/60}")    
        if payments_df is None or items_df is None:
            logger.error("One of the DataFrames failed to process. Exiting...")
            return

        # Step 2: Join Gift Card Info with final_header_df if available
        if not gift_card_info_df.isEmpty():
            logger.info("Joining Gift Card Info DataFrame with final_header_df")
            final_header_df = final_header_df.join(gift_card_info_df, on="Header_Id", how="left")
     
        try:
            # final_header_df = final_header_df.drop("Franchise")
            # final_header_df = final_header_df.withColumn("Franchise", col("StoreMaster_Franchise"))
            
            # final_header_df=final_header_df.withColumn('Franchise', upper(final_header_df['Franchise']))
            # final_header_df = final_header_df.na.fill({'Franchise': 'DIL'})
            
            # Step 3: Get Baseline data and add placeholder columns
            
            baseline_df = get_baseline_df(final_header_df, final_header_df_rename_mapping, baseline_columns)
            
            baseline_df=baseline_df.withColumn('Franchise', upper(baseline_df['Franchise']))
            baseline_df = baseline_df.na.fill({'Franchise': 'DIL'})
            
            final_baseline_df = add_placeholder_columns(baseline_df, ['GES_Code'])
            final_baseline_df = add_placeholder_columns(final_baseline_df, ['Transaction_Type'])
            # logger.info(f"final_baseline_df columns: {final_baseline_df}")
            
            final_items_df = add_placeholder_columns(items_df, ['Deal_Identifier'])
            final_payments_df = add_placeholder_columns(payments_df, ['Tender_Reference'])
            # logger.info(f"baseline Columns before save {baseline_df.columns}")
            logger.info("Before removing baseline_df, items_df, payments_df ")
            log_available_memory()
        except Exception as e:
            logger.error("Error in deleting baseline_df, items_df, payments_df",str(e))
            raise
        finally:
            del baseline_df
            del items_df
            del payments_df
            gc.collect()
            logger.info("After removing baseline_df, items_df, payments_df ")
            log_available_memory()
   
        
        
        try:
            # logger.info(f"final_baseline_df columns: {final_baseline_df.columns}")
            final_baseline_df = final_baseline_df \
               .withColumn("Invoice_Open_Date", date_format(to_date("Invoice_Open_Date", "yyyy/MM/dd"), "yyyy-MM-dd")) \
               .withColumn("Invoice_Closed_Date", date_format(to_date("Invoice_Closed_Date", "yyyy/MM/dd"), "yyyy-MM-dd")) \
               .withColumn("Business_Date", date_format(to_date("Business_Date", "yyyy/MM/dd"), "yyyy-MM-dd")) \
               .withColumn("System_Processed_Date", date_format(to_date("System_Processed_Date", "yyyy/MM/dd"), "yyyy-MM-dd")) \
               .withColumn("Enriched_Order_Date", date_format(to_date("Enriched_Order_Date", "yyyy/MM/dd"), "yyyy-MM-dd"))
        except Exception as e:
            logger.error(f"Error while handling date format for final_baseline_df: {e} ")
            logger.error(traceback.format_exc())
            raise ValueError(e)
           
        # Handle date format for final_payments_df
        try:
            final_payments_df=final_payments_df \
                .withColumn("Invoice_Open_Date", date_format(to_date("Invoice_Open_Date", "yyyy/MM/dd"), "yyyy-MM-dd")) \
                .withColumn("Tender_Date", date_format(to_date("Tender_Date", "yyyy/MM/dd"), "yyyy-MM-dd"))
        except Exception as e:
            logger.error(f"Error while handling date format for final_payments_df: {e} ")
            logger.error(traceback.format_exc())
            raise ValueError(e)
            raise
        
        # Handle date format for final_items_df
        try:
            final_items_df=final_items_df \
                .withColumn("Invoice_Open_Date", date_format(to_date("Invoice_Open_Date", "yyyy/MM/dd"), "yyyy-MM-dd")) 
        except Exception as e:
            logger.error(f"Error while handling date format for final_items_df: {e} ")
            logger.error(traceback.format_exc())
            raise ValueError(e)   
            
        # Save final_baseline_df using partition by  
        try:    
            baseline_with_partition_date = final_baseline_df.withColumn('partition_date', F.col('Enriched_Order_Date'))   
            baseline_output_path=f"{data_save_s3_path}{data_process_date}/BASELINE/"
            logger.info(f"Path to save baseline data : {baseline_output_path}")
            
            baseline_with_partition_date \
                .repartition("partition_date") \
                .write \
                .partitionBy("partition_date") \
                .mode("append") \
                .parquet(baseline_output_path)
                
            logger.info(f"Baseline data saved on path: {baseline_output_path}")    
        except Exception as e:
            logger.error(f"Error saving baseline data on path: {baseline_output_path} :\n {e} ")
            logger.error(traceback.format_exc())
            raise ValueError(e)
            
        # Save final_line_df using partition by  
        try:    
            line_with_partition_date = final_items_df.withColumn('partition_date', F.col('Invoice_Open_Date'))   
            line_output_path=f"{data_save_s3_path}{data_process_date}/LINE/"
            logger.info(f"Path to save item data : {line_output_path}")
            
            line_with_partition_date \
                .repartition("partition_date") \
                .write \
                .partitionBy("partition_date") \
                .mode("append") \
                .parquet(line_output_path)
            logger.info(f"Item data saved on path : {line_output_path}")    
        except Exception as e:
            logger.error(f"Error saving item data on path: {line_output_path} :\n {e} ")
            logger.error(traceback.format_exc())
            raise ValueError(e)    
        
        # Save final_payment_df using partition by  
        try:    
            payment_with_partition_date = final_payments_df.withColumn('partition_date', F.col('Invoice_Open_Date'))   
            payment_output_path=f"{data_save_s3_path}{data_process_date}/PAYMENT/"
            logger.info(f"Path to save payment data : {payment_output_path}")
            
            payment_with_partition_date \
                .repartition("partition_date") \
                .write \
                .partitionBy("partition_date") \
                .mode("append") \
                .parquet(payment_output_path)
            logger.info(f"Payment data saved on path : {payment_output_path}")    
        except Exception as e:
            logger.error(f"Error saving item data on path: {payment_with_partition_date} :\n {e} ")
            logger.error(traceback.format_exc())
            raise ValueError(e)        
        
        logger.info("Processed Data Saved Successfully")
        logger.info(f"Time taken to save BASELINE, payments and item data: {(time.time()-start_time)/60}")
        
        # Step 5: Process PII and Mapping Data concurrently
        start_time=time.time()
        with ThreadPoolExecutor() as executor:
            pii_future = executor.submit(process_pii_data, final_header_df, final_header_df_rename_mapping, pii_df_columns, raw_to_pii_column_rename_mapping, spark)
            mapping_future = executor.submit(process_mapping_records, final_header_df, final_header_df_rename_mapping, mapping_dataframe_columns, spark)

            for future in as_completed([pii_future, mapping_future]):
                try:
                    future.result()  # Handle any exceptions raised in the PII or Mapping processing
                except Exception as e:
                    logger.error("Data processing failed: %s", e)
                    subject = f"{job_name} - {type(e).__name__} in PII or Mapping Data Processing"
                    logger.error("Traceback: %s", traceback.format_exc())
                    send_sns_notification(f"Error in PII or Mapping Data Processing: {e}", subject, job_name, etl_start_time, SNS_TOPIC_ARN)

        logger.info("ETL process completed successfully.")
        logger.info(f"Time taken to save Mapping, PII data: {(time.time()-start_time)/60}")
    except Exception as e:
        # logger.error("Error in ETL process: %s", e)
        logger.error("Traceback: %s", traceback.format_exc())
        subject = f"{job_name} - {type(e).__name__} in ETL Process"
        send_sns_notification(f"Error in ETL process: {e}", subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        raise Exception(f"Error in ETL Process: {e}")

def get_final_header_data(spark,header_df,input_data_folder,data_save_s3_path, header_df_count, header_df_mandatory_data_columns,output_folder):
    """
    Retrieves and processes the final header data by filtering mandatory columns,
    processing invoice dates, tracking already processed data, and handling web order numbers.

    Parameters:
    - spark: Spark session object.
    - input_data_folder: The folder containing input data.
    - data_save_s3_path: S3 path for saving processed data.
    - header_df_count: The count of the header DataFrame.
    - header_df_mandatory_data_columns: List of mandatory columns for filtering.

    Returns:
    - final_header_df: Processed final header DataFrame or None if an error occurs.
    """
    final_header_df = None
    mandatory_missing_values_df = None
    try:
        # logger.info("Starting to retrieve and process final header data.")

        # Step 1: Filter mandatory columns
        header_df = filter_mandatory_columns(header_df,header_df_mandatory_data_columns, data_save_s3_path, output_folder)
        logger.info(f"header_df count:{header_df.count()}")
        
        log_available_memory()
        # Step 2: Process invoice dates
        header_df = process_invoice_dates(header_df)  
        if header_df is not None and not header_df.isEmpty():
            logger.info("Invoice dates processed successfully.")
            
            # Step 3: Track already processed data
            header_df = track_already_processed_data(header_df,header_df_count)

            if header_df is not None and not header_df.isEmpty():
                logger.info("Tracking of already processed data completed successfully.")
                # return header_df
                
                # Step 4: Process web order numbers
                final_header_df = process_web_order_numbers(header_df, data_save_s3_path)
                
                if final_header_df is not None:
                    logger.info("Final header data processed successfully.")
                    return final_header_df
                else:
                    logger.error("Failed to process web order numbers; final_header_df is None.")
            else:
                logger.warning("Header DataFrame is empty after tracking already processed data.")
        else:
            logger.warning("Header DataFrame is empty after processing invoice dates.")

    except (AnalysisException, Py4JJavaError) as spark_exception:
        logger.error("Spark-specific error in get_final_header_data: %s", spark_exception)
        logger.error("Traceback: %s", traceback.format_exc())
        subject = f"{job_name} - {type(e).__name__} in get_final_header_data()"
        send_sns_notification(f"Error while getting final_header_data : {e}", subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        raise Exception(f"Error in get_final_header_data(): {e}")
        
    except Exception as e:
        logger.error("Error in get_final_header_data: %s", e)
        logger.error("Traceback: %s", traceback.format_exc())
        subject = f"{job_name} - {type(e).__name__} in get_final_header_data()"
        send_sns_notification(f"Error while getting final_header_data : {e}", subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        raise Exception(f"Error in get_final_header_data(): {e}")
    finally:
        # Clean up resources if needed
        if header_df is not None:
            header_df.unpersist()
            del header_df
        if mandatory_missing_values_df is not None:
            mandatory_missing_values_df.unpersist()
            del mandatory_missing_values_df

    
            
def process_final_header_data(spark,final_header_df,output_folder, MISSING_COMBINATION_ARN, data_save_s3_path):
    """
    Processes the final header DataFrame, including channel mapping, column transformations,
    mobile number validation, and joining with store master data.

    Parameters:
    - spark: Spark session object.
    - final_header_df: The DataFrame containing final header data.

    Returns:
    - final_header_df: The processed final header DataFrame.
    """
    try:
        # Step 1: Channel Mapping
        try:
            logger.info("New Channels Mapping Started..!")
            dil_channels = get_dil_order_type_mapping_df(dil_channels_sheet, channels_mapping_excel_file)
            dil_channels_exceptions = get_dil_order_type_mapping_df(dil_exception_channel_sheet, channels_mapping_excel_file)
            mapping_table = get_mapping_table(channels_mapping_excel_file)
            
            final_header_df_mapped = get_enriched_order_type(final_header_df, dil_channels, mapping_table)
            final_header_df = add_exception_channels_to_df_with_weborder_no(final_header_df_mapped, dil_channels_exceptions, mapping_table)
            
            final_header_df = final_header_df.dropDuplicates(["Header_Id"])
            logger.info(f"final_header_df Count After Channel Mapping {final_header_df.count()}")
            try:
                missing_combination_ids = final_header_df.select('OrderSource','OrderType','Channel_Id').filter(col('source').isNull() & col('Channel').isNull() & col('Fulfilment_Mode').isNull()).distinct()
                # Convert DataFrame to dictionary
                df_dict = [row.asDict() for row in missing_combination_ids.collect()]
                if df_dict:
                    send_sns_notification(message=f"Hi Mohsin\nHere are The missing combination of orders:\n{df_dict}",subject="Missing Order Combination For DIL",job_name =job_name ,etl_start_time=etl_start_time, topic_arn=MISSING_COMBINATION_ARN)
                # Create a DataFrame for rows with missing Channel_Id
                missing_combination_ids_df = final_header_df.filter(col('source').isNull() & col('Channel').isNull() & col('Fulfilment_Mode').isNull()).distinct()
                
                if not missing_combination_ids_df.isEmpty():
                    logger.info(f'Found Missing Channel_Id So Saving Data and Not Processing Ahead {missing_combination_ids_df.count()}')
                    try:
                        # Save the missing combinations data to the specified S3 path
                        missing_combination_ids_df.write \
                                                  .option('header', True) \
                                                  .mode("append") \
                                                  .parquet(f"{data_save_s3_path}MISSING_CHANNELS_DATA/{output_folder}")
                        missing_combination_ids.write \
                                                  .option('header', True) \
                                                  .mode("append") \
                                                  .csv(f"{data_save_s3_path}MISSING_CHANNELS_DATA/IDS/{output_folder}")
                        logger.info(f"Missing Channels Data Saved at {data_save_s3_path}MISSING_CHANNELS_DATA/{output_folder}")
                        # logger.info(f"missing_combination_ids_df: {missing_combination_ids_df.count()}")
                        
                        # Remove rows with NULL Channel_Id from final_header_df
                        # final_header_df = final_header_df.filter(~(col('Enriched_Order_Type').isNull() & col('Enriched_Sales_Channel').isNull() & col('Fulfilment_Mode').isNull()))
                        
                        final_header_df = final_header_df.filter(~(col('Channel_Id').isNull()))
                        logger.info(f"final_header_df AFTER CHANNELS REMOVED: {final_header_df.count()}")
                    except Exception as e:
                        logger.info(traceback.format_exc())
                        raise
                    
            except Exception as e:
                logger.error("Failed to Send SNS Notification For Missing Channels")
                logger.error(traceback.format_exc())
                error_msg = f"Failed to Send SNS Notification For Missing Channels"
                subject = f"{job_name} - {type(e).__name__} In Channels Mapping Notification"
                send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                raise
        
        except Exception as e:
            subject = f"{job_name} - {type(e).__name__} In Channels Mapping"
            error_msg = f"{type(e).__name__} In Channels Mapping: {str(e)}"
            logger.error(error_msg)
            logger.error("Traceback: %s", traceback.format_exc())
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            raise
        
        finally:
            dil_channels.unpersist()
            dil_channels_exceptions.unpersist()
            mapping_table.unpersist()
            del dil_channels
            del dil_channels_exceptions
            del mapping_table

        # Step 2: Column Transformations
        if final_header_df:
            try:
                logger.info("Started Columns Transformation...!")
                final_header_df = final_header_df.dropDuplicates(["Header_Id"])
        
                # Renaming and processing columns
                final_header_df = final_header_df \
                    .withColumn("SGST", regexp_replace(col("SGST"), ",", "").cast("double")* -1) \
                    .withColumn("CGST", regexp_replace(col("CGST"), ",", "").cast("double")* -1) \
                    .withColumn("Tax_Amount", col("SGST") + col("CGST")) \
                    .withColumn("TotalOrderValue", regexp_replace(col("TotalOrderValue"), ",", "").cast("double")* -1) \
                    .withColumn("Totalorderqty", abs(regexp_replace(col("Totalorderqty"), ",", "").cast("int")))
        
                # Handling NetAmount column
                net_amount_col = col("NetAmount")
                if isinstance(final_header_df.schema['NetAmount'].dataType, StringType):
                    final_header_df = final_header_df.withColumn("NetAmount", regexp_replace(net_amount_col, ",", "").cast("double")* -1)
                elif isinstance(final_header_df.schema["NetAmount"].dataType, ArrayType):
                    final_header_df = final_header_df.withColumn("NetAmount", regexp_replace(net_amount_col[0], ",", "").cast("double")* -1)
                elif isinstance(final_header_df.schema["NetAmount"].dataType, StructType):
                    final_header_df = final_header_df.withColumn("NetAmount", regexp_replace(net_amount_col[0], ",", "").cast("double")* -1)
        
                # Handling Totaldiscount column
                total_discount_col = col("Totaldiscount")
                if isinstance(final_header_df.schema['Totaldiscount'].dataType, StringType):
                    final_header_df = final_header_df.withColumn("Totaldiscount", regexp_replace(total_discount_col, ",", "").cast("double")* -1)
                elif isinstance(final_header_df.schema["Totaldiscount"].dataType, ArrayType):
                    final_header_df = final_header_df.withColumn("Totaldiscount", regexp_replace(total_discount_col[0], ",", "").cast("double")* -1)
                elif isinstance(final_header_df.schema["Totaldiscount"].dataType, StructType):
                    final_header_df = final_header_df.withColumn("Totaldiscount", regexp_replace(total_discount_col[0], ",", "").cast("double")* -1)
        
                # Date transformations
                final_header_df = final_header_df \
                    .withColumn("Business_Date", col("InvoiceDate")) \
                    .withColumn("BillCloseddate", date_format(to_date(col("BillCloseddate"), "MM/dd/yy"), "yyyy/MM/dd")) \
                    .withColumn("Enriched_Order_Date", regexp_replace(coalesce(col("Order_Date"), col("InvoiceDate")), "-", "/")) 
                    
                    
                final_header_df = final_header_df\
                    .withColumn("Month", month(to_date(col("Enriched_Order_Date"), 'yyyy/MM/dd'))) \
                    .withColumn("Year", year(to_date(col("Enriched_Order_Date"), 'yyyy/MM/dd'))) \
                    .withColumn("Week_Day", week_day_udf(final_header_df["Enriched_Order_Date"])) \
                    .withColumn("Week_Number", week_number_udf(final_header_df["Enriched_Order_Date"])) \
                    .withColumn("System_Processed_Date", date_format(current_date(), "yyyy/MM/dd"))
                    
        
                # Time transformations
                final_header_df = final_header_df \
                    .withColumn('BillClosetime', convert_to_24hr_format_udf(final_header_df['BillClosetime'])) \
                    .withColumn('InvoiceTime', convert_to_24hr_format_udf(final_header_df['InvoiceTime'])) \
                    .withColumn("Enriched_Order_Time", coalesce(col("Order Time"), col("InvoiceTime")))
        
                # # Window function for Enriched_Order_Time
                # enrich_order_time_window = Window.orderBy('InvoiceDate', 'StoreCode','Enriched_Order_Time').rowsBetween(Window.unboundedPreceding, Window.currentRow)
                # final_header_df = final_header_df.withColumn('Enriched_Order_Time', last('Enriched_Order_Time', ignorenulls=True).over(enrich_order_time_window))
                try:
                    final_header_df = fill_null_enriched_order_time(final_header_df)
                except Exception as e:
                    print(traceback.format_exc)
                # # Day part and hour part calculations
                # final_header_df = final_header_df \
                #     .withColumn('Hour_Day_Part', get_hour_Day_Part_udf(final_header_df['Enriched_Order_Time'])) \
                #     .withColumn('Day_Part', get_day_part_udf(final_header_df['Enriched_Order_Time']))

                # Create Hour_Day_Part column based on Enriched_Order_Time
                final_header_df = final_header_df.withColumn('Hour_Day_Part', F.hour(col('Enriched_Order_Time')))
                
                # Create Day_Part column based on more granular time ranges
                final_header_df = final_header_df.withColumn(
                    "Day_Part",
                    when((col("Hour_Day_Part") >= 11) & (col("Hour_Day_Part") < 15), "Lunch")
                    .when((col("Hour_Day_Part") >= 15) & (col("Hour_Day_Part") < 19), "Snack")
                    .when((col("Hour_Day_Part") >= 19) & (col("Hour_Day_Part") < 23), "Dinner")
                    .when((col("Hour_Day_Part") >= 23) | (col("Hour_Day_Part") < 5), "Late night")
                    .when((col("Hour_Day_Part") >= 5) & (col("Hour_Day_Part") < 11), "Breakfast")
                    .otherwise(None)
                )

                # Valid transaction and brand columns
                final_header_df = final_header_df \
                    .withColumn("Valid_Transaction", when(col("EntryStatus") == "Voided", False).otherwise(True)) \
                    .withColumn("Pizzahut_Brand", F.when(F.col("Pasta by PH").startswith("PH"), F.lit("PizzaHut"))
                                .when(F.col("Pasta by PH").startswith("Pasta"), F.lit("PastaByPizzaHut"))
                                .when(F.col('Pasta by PH').isNull(), F.lit(None))
                                .otherwise(F.col("Pasta by PH"))) \
                    .withColumn("Enriched_Email_Id", coalesce(col("PHDV_Email"), col("Email"))) \
                    .withColumn('Non_Veg', when(col('ItemDetails.Items.FoodType').cast('string').contains('Non Veg'), True).otherwise(False)) \
                    .withColumn("Deal", F.expr("array_contains(transform(ItemDetails.Items.Promono, x -> x is not null), true)")) \
                    .withColumn("Ala_Carte", F.expr("array_contains(transform(ItemDetails.Items.Promono, x -> x is null), true)"))
            
            except Exception as e:
                subject = f"{job_name} - {type(e).__name__} During Column Transformations"
                error_msg = f"{type(e).__name__} during column transformations: {str(e)}"
                logger.error(error_msg)
                logger.error("Traceback: %s", traceback.format_exc())
                send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                raise ValueError(e)
        
        # Need to Process Mobile Number Validation independenty(concurrently)
        # Step 3: Mobile Number Validation
        try:
            logger.info("Started MobileNo Processing..!")
            phone_number_df_columns = ['Header_Id', "MobileNo", "PhoneNo", 'InvoiceDate']
            phone_number_df = final_header_df.select(*phone_number_df_columns)
            phone_number_processed_df = get_phone_number_processed(phone_number_df)
        except Exception as e:
            subject = f"{job_name} - {type(e).__name__} In Mobile Number Processing"
            error_msg = f'Error In phone Number Process: {str(e)}'
            logger.error(error_msg)
            logger.error("Traceback: %s", traceback.format_exc())
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            raise ValueError(e)
             
        # Step 4: Join with Store Master Data     
        try:
            logger.info("Started reading Store MasterDataframe..!") 
            STORE_MASTER_COLUMNS = ["FZE", "Champs", "City", "State", "Zone", "Cluster", "Format", "Location", "Tier", "Region",     "updated_code"]
 
            store_masters_df = get_store_master_df(spark,STORE_MASTER_COLUMNS)
            final_header_df = (final_header_df
                               .join(phone_number_processed_df, on="Header_Id", how='left')
                               .join(store_masters_df, final_header_df["StoreCode"] == store_masters_df["updated_code"], 
                               how="left"))
            final_header_df.persist(StorageLevel.MEMORY_AND_DISK)
            logger.info("Joined Store Master and Phone Number Processed Dataframe and created final Header Dataframe")
            # logger.info(f"final_header_df schema: {final_header_df.schema}")
            # logger.info(f"final_header_df Count: {final_header_df.count()}")
            return final_header_df
        except Exception as e:
            subject = f"{job_name} - {type(e).__name__}  In Store Master Dataframe"
            error_msg = f'Error In Store Master Dataframe: {str(e)}'
            logger.error(error_msg)
            logger.error("Traceback: %s", traceback.format_exc())
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            raise ValueError(e)
        finally:
            phone_number_df.unpersist()
            del phone_number_df
            store_masters_df.unpersist()
            del store_masters_df

        # try:
        #     final_header_df = final_header_df.drop(final_header_df.ItemDetails, final_header_df.PaymentsDetails)
        #     return final_header_df
        # except Exception as e:
        #     logger.info(traceback.format_exc())
        # finally:
        #     gc.collect()
        # return final_header_df
    except Exception as e:
        subject = f"{job_name} - {type(e).__name__} in process_final_header_data"
        error_msg = f"Exception in process_final_header_data(): {str(e)}"
        logger.error(error_msg)
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        raise ValueError(e)
    finally:
        gc.collect()
        
#####################################################################################################################################        
    
    
def main():
    try:
        log_available_memory()
        
        # data_save_s3_path = 's3://ph-etl-archive/DIL_CHANNEL_TEST/TEST/'
        data_save_s3_path="s3://ph-etl-archive/DIL_PROD_DATA_PARTITION/LIVE/"
        
        channel_save_s3_path="s3://ph-etl-archive/DIL_PROD_DATA/"
        # channel_save_s3_path="s3://ph-etl-archive/DIL_CHANNEL_TEST/TEST/"
        
        yesterday_date = (datetime.now() - timedelta(1)).strftime('%Y/%m/%d')
        # yesterday_date="2024/05/27"
        input_data_folder = f"dil/{yesterday_date}/"
        output_folder = input_data_folder.replace("dil/", "")
        try:
            header_df = spark.read.format('xml') \
                    .options(rowTag='CustomerInformation', excludeAttribute=True, treatEmptyValuesAsNulls=True) \
                    .schema(RAW_XML_DATA_SCHEMA) \
                    .load(f's3://{INPUT_DATA_BUCKET}/{input_data_folder}')
            header_df = header_df.dropDuplicates()
            # header_df = header_df.limit(100)
            header_df.persist(StorageLevel.MEMORY_AND_DISK)
            header_df_count = header_df.count()
            logger.info(f'Data Loaded for: s3://{INPUT_DATA_BUCKET}/{input_data_folder}\n'
                    f'Header DataFrame Count: {header_df_count}')
                    # f'Header DataFrame Columns: {header_df.columns}')
        except Py4JJavaError as e:
            # Check if the error was caused by java.io.IOException
            if "java.io.IOException" in str(e):
                logger.error(type(e).__name__)
                subject = f"{type(e).__name__} in main()"
                logger.error("Traceback: %s", traceback.format_exc())
                error_msg = f"Data Is not available for s3://{INPUT_DATA_BUCKET}/{input_data_folder}"
                send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
                sys.exit(1)
                # raise
            
        # except Exception as e:
        #     logger.info(traceback.format_exc())
        #     subject = f"{type(e).__name__} in main()"
        #     logger.error("Traceback: %s", traceback.format_exc())
        #     send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        #     raise 
        try:
            if header_df is None or not isinstance(header_df, DataFrame):
                raise ValueError("Loaded header_df is not a valid DataFrame in load_header_data()")
                # error_msg = f"Data Is not available for s3://{INPUT_DATA_BUCKET}/{input_data_folder}"
                # send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            # Step 2: Validate header_df
            if header_df and not header_df.isEmpty():
                log_dataframe_size(header_df, "Initial Header Dataframe")
                final_header_df = get_final_header_data(spark, header_df, input_data_folder, channel_save_s3_path, header_df_count, header_df_mandatory_data_columns,output_folder)
                try:
                    final_header_df = final_header_df.withColumn('Enriched_Order_Type', coalesce('Order Type', 'OrderType'))\
                                    .withColumn('Enriched_Sales_Channel', coalesce('New_Channel','OrderSource'))
                except Exception as e:
                    print(traceback.format_exc())
                    
                # Step 3: Process final header data
                final_header_df = process_final_header_data(spark, final_header_df,output_folder,MISSING_COMBINATION_ARN,channel_save_s3_path)
                # Step 4: Execute ETL process
                logger.info(f"final_header_df total rows: {final_header_df.count()}")
                if not final_header_df.isEmpty():
                    log_available_memory()
                    logger.info("Starting execute_etl_process")
                    execute_etl_process(final_header_df,payment_df_columns, payment_df_rename_mapping, item_df_columns, final_items_columns, data_save_s3_path, output_folder, final_header_df_rename_mapping, baseline_columns,input_data_folder,
                        yesterday_date,spark)
            else:
                logger.warning("final_header_df is empty after processing!")
                raise ValueError("final_header_df is empty after processing!")
        except Exception as e:
            logger.info(traceback.format_exc())
            subject = f"{type(e).__name__} in main()"
            error_msg = "Eroor Is Stating Job"
            logger.error("Traceback: %s", traceback.format_exc())
            send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
            raise
        
    except Exception as e:
        error_msg = f"Exception encountered in main(): {str(e)}"
        subject = f"{type(e).__name__} in main()"
        logger.error("Traceback: %s", traceback.format_exc())
        send_sns_notification(error_msg, subject, job_name, etl_start_time, SNS_TOPIC_ARN)
        raise RuntimeError(f"Error in main(): {e}")

    finally:
        try:
            log_output_folder_name = f"{data_save_s3_path.replace('s3://ph-etl-archive/', '')}Logs/{output_folder}"
            file_name = f"{job_name}_{output_folder.replace('/', '_').strip()}"
            log_path = Save_log_file(logger, log_stream, s3_client, bucket=OUTPUT_DATA_BUCKET, log_output_folder=log_output_folder_name, log_file_name=file_name)

            message = f'Logs saved at: {log_path} \n for: {input_data_folder}'
            subject = "DIL ETL Log: response"
            send_sns_notification(message, subject, job_name, etl_start_time, SNS_TOPIC_ARN)

        except Exception as e:
            logger.error('Error saving log file: %s', e)
            logger.error(f"Traceback: {traceback.format_exc()}")
            message = f"Error in log file saving: {e}"
            subject = "ETL response"
            send_sns_notification(message, subject, job_name, etl_start_time, SNS_TOPIC_ARN)

if __name__ == "__main__":
    main()
    job.commit()    
    
