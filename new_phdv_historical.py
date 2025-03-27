
from config import *
from custom_loggers import *

# Define a custom S3 File Not Found exception
class S3FileNotFoundException(Exception):
    def __init__(self, file_path, bucket_name):
        self.file_path = file_path
        self.bucket_name = bucket_name
        super().__init__(f"S3 file '{file_path}' not found in bucket '{bucket_name}'.")

def generate_dates_for_phdv_dataframe_for_live(start_date: str, num_days: int):
    """Generate a list of dates from num_days before the given start_date (inclusive of start_date), in ascending order."""
    try:
        base_date = datetime.strptime(start_date, '%Y/%m/%d')  # Parse input date
        # Generate list of dates starting from (base_date - num_days) to base_date in ascending order
        date_list = [(base_date - timedelta(days=i)).strftime('%Y/%m/%d') for i in range(num_days, -1, -1)]
        return date_list
    except Exception as e:
        print(f"Error generating dates: {e}")
        return []
    
def generate_dates_for_phdv_dataframe(start_date: str, num_days: int) -> List[str]:
    """Generate a list of dates around a given start date."""
    try:
        base_date = datetime.strptime(start_date, '%Y/%m/%d')
        date_list = [(base_date + timedelta(days=i)).strftime('%Y/%m/%d') for i in range(-num_days, num_days + 1)]
        return date_list
    except Exception as e:
        logger.error(f"Error generating dates: {e}")
        return []

def read_and_merge_parquet_from_s3_spark(bucket_name: str, date_list: List[str], spark: SparkSession, franchise: str, phdv_schema: StructType) -> Tuple[DataFrame, List[str]]:
    """Read and merge Parquet files from S3 into a Spark DataFrame, tracking dates with no data."""
    df_list = []

    for date in date_list:
        year, month, day = date.split('/')
        prefix = f"phdv-data/{year}/{month}/{day}/"
        
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
            if parquet_files:
                for parquet_key in parquet_files:
                    s3_path = f"s3a://{bucket_name}/{parquet_key}"
                    df = spark.read.schema(phdv_schema).parquet(s3_path)
                    filtered_df = df.filter(df['Franchise'] == franchise)
                    df_list.append(filtered_df)
            else:
                # If the prefix exists but no parquet files are present
                logger.info(f"No Parquet files found for date: {date}")
                raise S3FileNotFoundException(f"{bucket_name}/{prefix}")
        else:
            # If no objects at all under the prefix
            logger.info(f"No PHDV data available for: {prefix}")
            raise S3FileNotFoundException(f"{bucket_name}/{prefix}")

    if df_list:
        master_phfv_df = df_list[0]
        for df in df_list[1:]:
            master_phfv_df = master_phfv_df.unionByName(df)
        return master_phfv_df
    else:
        return spark.createDataFrame([], schema=phdv_schema)

def process_phdv_dataframe(invoice_date: str, days_to_process_phdv: int, header_df_with_webOrder_number: DataFrame, spark: SparkSession, franchise: str, phdv_schema: StructType) -> Tuple[DataFrame, DataFrame, List[str]]:
    """Processes PHDV data for a specific invoice date and returns matching, non-matching DataFrames, and dates with no data."""
    try:
        # Generate date range for processing
        logger.info(f"Getting Dates Generated from generate_dates_for_phdv_dataframe with {invoice_date}, {days_to_process_phdv}")
        # date_list = generate_dates_for_phdv_dataframe_for_live(invoice_date, days_to_process_phdv)
        date_list = generate_dates_for_phdv_dataframe(invoice_date, days_to_process_phdv)
        try:
            # Load and merge PHDV data from S3
            phdv_dataframe = read_and_merge_parquet_from_s3_spark("ph-etl-archive", date_list, spark, franchise, phdv_schema)
        
        except S3FileNotFoundException  as e:
            print(f"Error: {e}")
            exit(1)
            
        except Exception as e:
            print(traceback.format_exc())
            raise
            
        logger.info(f'Date List: {date_list}')
        logger.info(f"PHDV Data Count: {phdv_dataframe.count()}")

        if phdv_dataframe.rdd.isEmpty():
            return spark.createDataFrame([], schema=header_df_with_webOrder_number.schema), spark.createDataFrame([], schema=header_df_with_webOrder_number.schema)

        # Preprocessing PHDV data
        phdv_dataframe = phdv_dataframe.withColumn("Order_Date", regexp_replace(col("Order_Date"), "-", "/"))
        phdv_dataframe = phdv_dataframe.withColumnRenamed("Email", "phdv_Email")
        phdv_dataframe.cache()

        # Filtering header DataFrame for the specific invoice date
        temp_header_df = header_df_with_webOrder_number.filter(col('InvoiceDate') == invoice_date).dropDuplicates(subset=["Header_Id"])
        temp_header_df.cache()

        logger.info(f"Data for {invoice_date}: {temp_header_df.count()}")

        # Joining PHDV data with header data for same-day matches
        same_day_match_df = phdv_dataframe.join(
            temp_header_df,
            (phdv_dataframe["Order_Date"] == temp_header_df["InvoiceDate"]) &
            (phdv_dataframe["Store Code"] == temp_header_df["StoreCode"]) &
            (phdv_dataframe["OrderNo"] == temp_header_df["Enriched_WebOrder_No"]),
            how="inner"
        ).dropDuplicates(subset=["Header_Id"])

        logger.info(f'Same Day Match Count: {same_day_match_df.count()}')

        # Extracting matched header IDs for same-day matches
        same_day_matched_header_ids = same_day_match_df.select("Header_Id").distinct()

        # Identifying non-matched headers for the same day
        same_day_non_matched_df = temp_header_df.join(same_day_matched_header_ids, on="Header_Id", how="left_anti")

        # Joining PHDV data with remaining non-matched headers for alternative day matches
        alternative_day_match_df = phdv_dataframe.join(
            same_day_non_matched_df,
            (phdv_dataframe["Store Code"] == same_day_non_matched_df["StoreCode"]) &
            (phdv_dataframe["OrderNo"] == same_day_non_matched_df["Enriched_WebOrder_No"]) &
            (phdv_dataframe["Order_Date"] != same_day_non_matched_df["InvoiceDate"]),
            how="inner"
        ).dropDuplicates(subset=["Header_Id"])

        logger.info(f'Alternative Day Match Count: {alternative_day_match_df.count()}')

        # Extracting matched header IDs for alternative day matches
        alternative_day_matched_header_ids = alternative_day_match_df.select("Header_Id").distinct()

        # Identifying the last remaining headers that did not match any PHDV record
        last_remaining_header_df = same_day_non_matched_df.join(
            alternative_day_matched_header_ids, 
            on="Header_Id", 
            how="left_anti"
        )

        logger.info(f'Last Remaining Header Count: {last_remaining_header_df.count()}')

        # Combine same-day and alternative-day matches into a single DataFrame
        matching_and_non_matching_data = same_day_match_df.unionByName(alternative_day_match_df, allowMissingColumns=True).dropDuplicates(subset=["Header_Id"])

        logger.info(f'Same & Alternative Match Count: {matching_and_non_matching_data.count()}')

        # Identifying records missing in the PHDV data
        missing_records_df = temp_header_df.join(
            matching_and_non_matching_data,
            on=["StoreCode", "InvoiceDate", "Enriched_WebOrder_No"],
            how="left_anti"
        ).dropDuplicates(subset=["Header_Id"])

        # Further filtering the missing records to ensure they are indeed unmatched
        missing_records_df = missing_records_df.unionByName(last_remaining_header_df).dropDuplicates(subset=["Header_Id"])

        logger.info(f'Missing Records Count: {missing_records_df.count()}')

        return matching_and_non_matching_data, missing_records_df

    except Exception as e:
        logger.info(f"Error in process_phdv_dataframe: {e}")
        return spark.createDataFrame([], schema=header_df_with_webOrder_number.schema), spark.createDataFrame([], schema=header_df_with_webOrder_number.schema), []

def get_phdv_merged_df(header_df_with_webOrder_number: DataFrame, date_df: DataFrame, 
    days_to_process_phdv: int, spark: SparkSession, franchise: str, phdv_schema: StructType) -> Tuple[DataFrame, DataFrame, List[str]]:
    """Processes all given invoice dates to produce a merged DataFrame with matching and non-matching records, and tracks dates with no PHDV data."""
    
    # Validation checks for input arguments
    if header_df_with_webOrder_number is None or not isinstance(header_df_with_webOrder_number, DataFrame):
        raise ValueError("header_df_with_webOrder_number should be a non-null DataFrame")

    if date_df is None or not isinstance(date_df, DataFrame) or date_df.rdd.isEmpty():
        raise ValueError("Invoice Dates should be a non-empty DataFrame")

    if not isinstance(days_to_process_phdv, int) or days_to_process_phdv <= 0:
        raise ValueError("days_to_process_phdv should be a positive integer")

    all_dates_merged_data = None
    record_did_not_match = None
    invoice_date_list = [row.InvoiceDate for row in date_df.select("InvoiceDate").collect()]

    provided_count = header_df_with_webOrder_number.count()
    logger.info(f'Provided Data: {provided_count}')

    # Iterating through each invoice date
    for invoice_date in invoice_date_list:
        logger.info(f"=============================== Processing {invoice_date} ===============================")
        try:
            # Processing PHDV data for the current invoice date
            matching_and_non_matching_data, missing_records_df = process_phdv_dataframe(invoice_date, days_to_process_phdv, header_df_with_webOrder_number, spark, franchise, phdv_schema)

            # Combining matched data for all dates
            if matching_and_non_matching_data and not matching_and_non_matching_data.rdd.isEmpty():
                if all_dates_merged_data is None:
                    all_dates_merged_data = matching_and_non_matching_data
                else:
                    all_dates_merged_data = all_dates_merged_data.unionByName(matching_and_non_matching_data, allowMissingColumns=True)

            # Combining non-matched data for all dates
            if missing_records_df and not missing_records_df.rdd.isEmpty():
                if record_did_not_match is None:
                    record_did_not_match = missing_records_df
                else:
                    record_did_not_match = record_did_not_match.unionByName(missing_records_df, allowMissingColumns=True)

        except Exception as e:
            logger.info(f"Error processing date {invoice_date}: {e}")

    # Dropping duplicates from the final DataFrame of matched data
    if all_dates_merged_data is not None:
        phdv_merged_header_df = all_dates_merged_data.dropDuplicates(subset=["Header_Id"])
    else:
        phdv_merged_header_df = spark.createDataFrame([], header_df_with_webOrder_number.schema)

    # Dropping duplicates from the final DataFrame of non-matched data
    if record_did_not_match is not None:
        record_did_not_match = record_did_not_match.dropDuplicates(subset=["Header_Id"])
    else:
        record_did_not_match = spark.createDataFrame([], header_df_with_webOrder_number.schema)

    logger.info("===" * 30)
    phdv_merged_header_df_count = phdv_merged_header_df.count() if phdv_merged_header_df else 0
    record_did_not_match_count = record_did_not_match.count() if record_did_not_match else 0
    returned_count = phdv_merged_header_df_count + record_did_not_match_count
    logger.info(f"ALL PHDV MERGED: {phdv_merged_header_df_count}")
    logger.info(f"ALL PHDV MERGED: {record_did_not_match_count}")
    logger.info(f"TOTAL DATA RETURNED: {returned_count}")
    
    return phdv_merged_header_df, record_did_not_match
