from config import *
from custom_loggers import *
from sns_config import *

def get_store_master_df(spark: DataFrame, STORE_MASTER_COLUMNS:List) -> DataFrame:
    store_master_df_path = 's3://ph-etl-archive/Master_Table/StoreMaster/*'
    store_master_df = spark.read.parquet(store_master_df_path)
    store_master_df = store_master_df.select(*STORE_MASTER_COLUMNS)
    store_master_df = store_master_df.withColumnRenamed('FZE', 'StoreMaster_Franchise')
    store_master_df = store_master_df.withColumnRenamed('Champs', 'StoreMaster_Champ_Id')
    store_master_df = store_master_df.withColumnRenamed('City', 'StoreMaster_Store_City')
    store_master_df = store_master_df.withColumnRenamed('State', 'StoreMaster_State')
    store_master_df = store_master_df.withColumnRenamed('Cluster', 'StoreMaster_Cluster')
    store_master_df = store_master_df.withColumnRenamed('Format', 'StoreMaster_Format')
    store_master_df = store_master_df.withColumnRenamed('Location', 'StoreMaster_Location')
    store_master_df = store_master_df.withColumnRenamed('Tier', 'StoreMaster_Tier_Category')
    store_master_df = store_master_df.withColumnRenamed('Region', 'StoreMaster_Region')
    store_master_df = store_master_df.withColumnRenamed('S_Name', 'StoreMaster_StoreName')
    store_master_df = store_master_df.withColumnRenamed('Zone', 'StoreMaster_Zone')
    # Ensure store_master_df has unique entries for updated_code
    store_master_unique = store_master_df.dropDuplicates(["updated_code"])
    return store_master_unique

def get_enriched_webOrderNumber(web_order_no):
    """
    Corrects web order numbers that end with 'P' (case insensitive) or '0' 
    under specific conditions.

    Parameters:
    web_order_no (str): The web order number to be corrected.

    Returns:
    str: The corrected web order number.

    Correction Rules:
    A: If the web order number ends with 'P' (case insensitive), the last character is removed.
    B: If the web order number is 9 characters long and ends with '0', the last character is removed.
    """
    web_order_no = str(web_order_no)
    # Check if web_order_no ends with 'P' (case insensitive)
    if web_order_no.endswith('P') or web_order_no.endswith('p'):
        return web_order_no[:-1]

    # Check if web_order_no is 9 characters long and ends with '0'
    if len(web_order_no) == 9 and web_order_no.endswith('0'):
        return web_order_no[:-1]

    # Return the web_order_no as it is if no conditions are met
    return web_order_no


# Define the function to map time to the appropriate time range
def get_hour_Day_Part(trans_time):
    """
    Determines the hour part of the day for a given transaction time.

    This function takes a time input in the format 'HH:MM:SS' and returns an 
    integer representing the hour range for the transaction time. Each hour 
    interval corresponds to a unique integer value from 0 to 23.

    Parameters:
        trans_time (str or None): A string representing the transaction time 
        in 'HH:MM:SS' format. If the input is None, the function returns None.

    Returns:
        int or None: An integer representing the hour range corresponding to 
                    the transaction time, where:
                    - 0 represents "00:00:00 to 00:59:59"
                    - 1 represents "01:00:00 to 01:59:59"
                    - ...
                    - 23 represents "23:00:00 to 23:59:59"
                    Returns None if the time is None.
    Notes:
        - The function assumes that input times are provided in 24-hour format.
        - Each hour from midnight (00:00) to the end of the day (23:59) is 
        mapped to its corresponding hour number.
    """
    # Return None if the transaction time is None
    if trans_time is None:
        return None

    # Dictionary mapping each hour of the day to its corresponding hour number
    time_ranges = {
        "1": [("01:00:00", "01:59:59")],
        "2": [("02:00:00", "02:59:59")],
        "3": [("03:00:00", "03:59:59")],
        "4": [("04:00:00", "04:59:59")],
        "5": [("05:00:00", "05:59:59")],
        "6": [("06:00:00", "06:59:59")],
        "7": [("07:00:00", "07:59:59")],
        "8": [("08:00:00", "08:59:59")],
        "9": [("09:00:00", "09:59:59")],
        "10": [("10:00:00", "10:59:59")],
        "11": [("11:00:00", "11:59:59")],
        "12": [("12:00:00", "12:59:59")],
        "13": [("13:00:00", "13:59:59")],
        "14": [("14:00:00", "14:59:59")],
        "15": [("15:00:00", "15:59:59")],
        "16": [("16:00:00", "16:59:59")],
        "17": [("17:00:00", "17:59:59")],
        "18": [("18:00:00", "18:59:59")],
        "19": [("19:00:00", "19:59:59")],
        "20": [("20:00:00", "20:59:59")],
        "21": [("21:00:00", "21:59:59")],
        "22": [("22:00:00", "22:59:59")],
        "23": [("23:00:00", "23:59:59")],
        "0": [("00:00:00", "00:59:59")]
    }

    # Iterate through each hour and its ranges
    for hour_number, ranges in time_ranges.items():
        # Check each time range within the current hour
        for start, end in ranges:
            # Compare the transaction time to determine if it falls within the current range
            if start <= trans_time <= end:
                # Return the corresponding hour number as an integer
                return int(hour_number)

    # In case no match is found, return None (should not occur with valid input)
    return None

def get_day_part(trans_time):
    """
    Determines the part of the day for a given transaction time in 24-hour format.

    This function accepts a time string in 'HH:MM:SS' 24-hour format and returns a
    string representing the part of the day according to the following categories:
    
    - Lunch: 11:00:00 - 14:59:59
    - Snack: 15:00:00 - 18:59:59
    - Dinner: 19:00:00 - 22:59:59
    - Late night: 23:00:00 - 03:59:59
    - Breakfast: 04:00:00 - 10:59:59

    Parameters:
        trans_time (str or None): A string representing the transaction time 
                                  in 'HH:MM:SS' format. If the input is None, 
                                  the function returns None.

    Returns:
        str or None: A string representing the part of the day corresponding to 
                     the transaction time, or None if the format is incorrect.

    Example:
        >>> get_day_part("21:30:00")
        'Dinner'
        >>> get_day_part("13:45:00")
        'Lunch'
        >>> get_day_part("04:15:00")
        'Breakfast'
        >>> get_day_part(None)
        None
    """
    if trans_time is None:
        return None

    try:
        # Convert 24-hour time string to a time object
        time_24hr = datetime.strptime(trans_time, '%H:%M:%S').time()

        # Define day parts with their time ranges
        day_parts = {
            "Breakfast": (time(4, 0, 0), time(10, 59, 59)),
            "Lunch": (time(11, 0, 0), time(14, 59, 59)),
            "Snack": (time(15, 0, 0), time(18, 59, 59)),
            "Dinner": (time(19, 0, 0), time(22, 59, 59)),
            "Late night": (time(23, 0, 0), time(3, 59, 59))
        }

        # Determine the correct day part
        for part, (start, end) in day_parts.items():
            # Handle "Late night" separately as it wraps over midnight
            if part == "Late night":
                if time_24hr >= start or time_24hr <= end:
                    return part
            elif start <= time_24hr <= end:
                return part
    except ValueError:
        return None
    
    return None


def convert_to_24hr_format(time_str: Optional[str]) -> Optional[str]:
    """
    Converts a time string to a 24-hour formatted time string.

    Supports both 12-hour format (e.g., '11:41:56 PM') and 24-hour format (e.g., '21:20:11').

    Parameters:
    time_str (Optional[str]): A time string in either 12-hour or 24-hour format.

    Returns:
    Optional[str]: The time string converted to 24-hour format (e.g., '23:41:56').
    Returns None if the input is None or if the input format is invalid.
    """
    if time_str is None:
        return None

    time_str = time_str.strip()

    # Check for 24-hour format
    try:
        return datetime.strptime(time_str, '%H:%M:%S').strftime('%H:%M:%S')
    except ValueError:
        pass

    # Check for 12-hour format
    try:
        return datetime.strptime(time_str, '%I:%M:%S %p').strftime('%H:%M:%S')
    except ValueError:
        return None
    
    
# Function to clean phone number based on specified conditions
def clean_phone_number(mobile_number):
    if mobile_number is not None:
        # Remove spaces
        mobile_number = mobile_number.replace(" ", "").strip()

        # Condition 2: Pick MobileNo of length 11
        if len(mobile_number) == 11 and mobile_number[0] == '0':
            return mobile_number[1:]  # Remove leading 0

        elif len(mobile_number) < 10:
            # logger.info(f'Lenghth Of Mobile :{len(mobile_number)}')
            return mobile_number

        # Condition 4: Pick MobileNo of length 13
        elif len(mobile_number) == 12 and mobile_number.startswith('91'):
            return mobile_number[2:]  # Remove leading +91

        # Condition 4: Pick MobileNo of length 13
        elif len(mobile_number) == 13 and mobile_number.startswith('+91'):
            return mobile_number[3:]  # Remove leading +91

        elif len(mobile_number) == 10:
            return mobile_number
        else:
            return mobile_number

    return mobile_number

# Define a function to clean and validate mobile numbers


def clean_and_validate_mobile_number(mobile_number, transaction_count):
    clean_number = None
    validation_tag = False

    if mobile_number is not None:
        # Clean the mobile number
        cleaned_number = clean_phone_number(mobile_number)
        logger.info(f"Cleaned Number: {cleaned_number}")

        # Condition 7: Check if transactions today exceed 4
        if transaction_count >= 4:
            clean_number = cleaned_number
            validation_tag = False

        # Check if cleaned_number is in invalid_numbers list
        invalid_numbers = ['9999999999', '8888888888','7777777777', '6666666666', '9876543210']
        if clean_number in invalid_numbers:
            clean_number = None
            validation_tag = False

        # Condition 1: Pick MobileNo of length 10
        if len(cleaned_number) == 10 and cleaned_number[0] in ['9', '8', '7', '6']:
            clean_number = cleaned_number
            validation_tag = True

        # Condition 3: Pick MobileNo of length 12
        elif re.match(r'^91[6789]\d{10}$', cleaned_number):
            clean_number = cleaned_number[2:]  # Remove leading 91
            validation_tag = True
        elif len(cleaned_number) < 10:
            clean_number = cleaned_number
            validation_tag = False
        else:
            clean_number = cleaned_number
            validation_tag = False

    # Condition 7: Check if transactions today exceed 4
    if transaction_count >= 4 and clean_number is not None:
        clean_number = cleaned_number
        validation_tag = False

    # Return cleaned number and validation flag
    return clean_number, validation_tag


def clean_phone_number(mobile_number: str) -> str:
    if mobile_number is not None:
        # Remove spaces
        mobile_number = mobile_number.replace(" ", "").strip()

        # Condition 2: Pick MobileNo of length 11
        if len(mobile_number) == 11 and mobile_number[0] == '0':
            return mobile_number[1:]  # Remove leading 0

        elif len(mobile_number) < 10:
            # logger.info(f'Lenghth Of Mobile :{len(mobile_number)}')
            return mobile_number

        # Condition 4: Pick MobileNo of length 13
        elif len(mobile_number) == 12 and mobile_number.startswith('91'):
            return mobile_number[2:]  # Remove leading +91

        # Condition 4: Pick MobileNo of length 13
        elif len(mobile_number) == 13 and mobile_number.startswith('+91'):
            return mobile_number[3:]  # Remove leading +91

        elif len(mobile_number) == 10:
            return mobile_number
        else:
            return mobile_number

    return mobile_number
# Define a function to clean and validate mobile numbers


def clean_and_validate_mobile_number(mobile_number, transaction_count):
    clean_number = None
    validation_tag = False

    if mobile_number is not None:
        # Clean the mobile number
        cleaned_number = clean_phone_number(mobile_number)
        logger.info(f"Cleaned Number: {cleaned_number}")

        # Condition 7: Check if transactions today exceed 4
        if transaction_count >= 4:
            clean_number = cleaned_number
            validation_tag = False

        # Check if cleaned_number is in invalid_numbers list
        invalid_numbers = ['9999999999', '8888888888','7777777777', '6666666666', '9876543210']
        if clean_number in invalid_numbers:
            clean_number = None
            validation_tag = False

        # Condition 1: Pick MobileNo of length 10
        if len(cleaned_number) == 10 and cleaned_number[0] in ['9', '8', '7', '6']:
            clean_number = cleaned_number
            validation_tag = True

        # Condition 3: Pick MobileNo of length 12
        elif re.match(r'^91[6789]\d{10}$', cleaned_number):
            clean_number = cleaned_number[2:]  # Remove leading 91
            validation_tag = True
        elif len(cleaned_number) < 10:
            clean_number = cleaned_number
            validation_tag = False
        else:
            clean_number = cleaned_number
            validation_tag = False

    # Condition 7: Check if transactions today exceed 4
    if transaction_count >= 4 and clean_number is not None:
        clean_number = cleaned_number
        validation_tag = False

    # Return cleaned number and validation flag
    return clean_number, validation_tag

def get_week_number(date_str):
    """
    Calculates the week number for a given date string.

    This function takes a date string formatted as 'YYYY/MM/DD' and computes the 
    week number of the year, considering weeks that start on Monday. The first 
    week is defined as the one containing the first Thursday of the year, 
    following ISO-8601 week date system.

    Parameters:
        date_str (str): A string representing a date in the format 'YYYY/MM/DD'.
    
    Returns:
        int: The week number of the year as an integer, where 1 represents the first week.
    
    Raises:
        ValueError: If the provided date_str is not in the 'YYYY/MM/DD' format 
                    or represents an invalid date.

    Notes:
        - The function assumes that weeks start on Monday.
        - The first week is defined as the one containing the first Thursday of the year, aligning with ISO-8601.
        - This may differ from calendar weeks in some countries or systems where weeks start on Sunday.

    """
    # Convert the input date string to a datetime object
    # The format '%Y/%m/%d' expects the date in 'YYYY/MM/DD' format.
    date = datetime.strptime(date_str, '%Y/%m/%d')
    
    # Calculate the first day of the year for the given date
    # This sets up a reference point to calculate the week number.
    start_of_year = datetime(date.year, 1, 1)
    
    # Find the start of the first week of the year
    first_week_start = start_of_year - timedelta(days = start_of_year.weekday())
    
    # Calculate the week number
    # The difference in days between the given date and the start of the first week
    # is divided by 7 to determine the week number, with an additional 1 to align
    # with the convention that the first week is week 1.
    week_number = ((date - first_week_start).days // 7) + 1
    return week_number


def get_week_day(date_str):
    # Parse the date string
    try:
        date_obj = datetime.strptime(date_str, "%Y/%m/%d")
        # Get the weekday (0=Monday, 6=Sunday)
        weekday = date_obj.weekday()
        # Convert to desired format (1=Monday, 7=Sunday)
        return weekday + 1
    except ValueError:
        return None


def startup_function(bucket_name, file_key):
    # Read CSV file from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj['Body'])

    # Ensure the 'dates' column exists
    if 'dates' not in df.columns:
        raise KeyError(f"'dates' column not found in the CSV file: {file_key}")

    # Get the last date
    last_date = df['dates'].iloc[-1]

    # Convert to yyyy/mm/dd format
    formatted_date = pd.to_datetime(last_date).strftime('%Y/%m/%d')

    return formatted_date

# Function to update the date in the CSV file in S3


def closing_function(bucket_name, file_key):
    # Read CSV file from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj['Body'])

    # Ensure the 'dates' column exists
    if 'dates' not in df.columns:
        raise KeyError(f"'dates' column not found in the CSV file: {file_key}")

    # Get the current date
    current_date = datetime.now().strftime('%Y/%m/%d')

    # Append the current date to the DataFrame
    df = df.append({'dates': current_date}, ignore_index=True)

    # Convert DataFrame back to CSV
    csv_buffer = df.to_csv(index=False)

    # Upload updated CSV to S3
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer)


def add_empty_columns(df: DataFrame, num_columns: int) -> DataFrame:
    """
    Adds a specified number of empty columns to a DataFrame.

    Parameters:
    df (DataFrame): The input DataFrame.
    num_columns (int): The number of empty columns to add.

    Returns:
    DataFrame: The DataFrame with the added empty columns.
    """
    for i in range(num_columns):
        df = df.withColumn(f'Empty_Column_{i+1}', lit(None).cast("string"))
    return df


def convert_date_format(date_str):
    """
    Converts a date string to the format YYYY/MM/DD.

    Args:
        date_str (str): The date string in any recognized format.

    Returns:
        str: The date in YYYY/MM/DD format, or an error message if parsing fails.
    """
    try:
        # Attempt parsing with common formats first
        formats = ["%d-%m-%y", "%d-%m-%Y", "%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"]
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y/%m/%d')
            except ValueError:
                continue

        # Fallback to dateutil.parser for other formats
        parsed_date = parser.parse(date_str)
        return parsed_date.strftime('%Y/%m/%d')

    except Exception as e:
        return f"Error: {e}"


def read_mapping_files(spark, mapping_combination_filename,order_combination_filename) -> (DataFrame, DataFrame):
    """
    Read mapping files from an Excel file into Spark DataFrames.

    Args:
    - filename (str): Path to the Excel file.

    Returns:
    - order_combination_df (DataFrame): Spark DataFrame containing 'order_combination' data.
    - maping_combination_df (DataFrame): Spark DataFrame containing 'maping_combination' data.
    """
    # order_combination_pd = pd.read_excel(filename, sheet_name='order_combination', index_col=None, dtype={'OrderSource_map': str, 'OrderType_map': str, 'Combination_Id': str})
    # # order_combination_pd['OrderSource_map'] = order_combination_pd['OrderSource_map'].astype(str)
    # # order_combination_pd['OrderType_map'] = order_combination_pd['OrderType_map'].astype(str)
    # # order_combination_pd['Combination_Id'] = order_combination_pd['Combination_Id'].astype(str)
    
    # order_combination_df = spark.builder.getOrCreate().createDataFrame(order_combination_pd)

    # maping_combination_pd = pd.read_excel(filename, sheet_name='maping_combination', index_col=None, dtype={'Enriched_Order_Type': str, 'Enriched_Channel': str, 'Fulfilment_Mode': str, "Combination_Id": str})
    # # maping_combination_pd['Enriched_Order_Type'] = maping_combination_pd['Enriched_Order_Type'].astype(str)
    # # maping_combination_pd['Enriched_Channel'] = maping_combination_pd['Enriched_Channel'].astype(str)
    # # maping_combination_pd['Combination_Id'] = maping_combination_pd['Combination_Id'].astype(str)
    # maping_combination_df = spark.builder.getOrCreate().createDataFrame(maping_combination_pd)
    # Read XML data from S3 with treatEmptyValuesAsNulls option
    maping_combination_df = spark.read.format('csv').options(header=True).load(mapping_combination_filename)
    # Read XML data from S3 with treatEmptyValuesAsNulls option
    order_combination_df = spark.read.format('csv').options(header=True).load(order_combination_filename)

    return order_combination_df, maping_combination_df


def get_enriched_order_type(df: DataFrame, spark, mapping_combination_filename,order_combination_filename) -> DataFrame:
    """
    Perform case-insensitive joins with mapping DataFrames on `OrderSource`, `OrderType`, and `Combination_Id`.

    Args:
    - df (DataFrame): Input Spark DataFrame containing `OrderSource` and `OrderType` columns.
    - excel_file_path (str): Path to the Excel file containing mapping sheets.

    Returns:
    DataFrame: Resulting DataFrame after performing joins with `order_combination_df` and `maping_combination_df`.
    """
    order_combination_df, maping_combination_df = read_mapping_files(spark, mapping_combination_filename,order_combination_filename)
    
    # Perform case-insensitive join on order_combination_df
    try:
        df = df.join(order_combination_df,
            (lower(df['OrderSource']) == lower(order_combination_df['OrderSource_map'])) &
            (lower(df['OrderType']) == lower(order_combination_df['OrderType_map'])),how='left')
    except Exception as e:
        logger.info(f'Error in Order Combination Merge')
    # logger.info(f"Count inside function After Combination id creation: {df.count()}")
    # # Perform left join on maping_combination_df using Combination_Id
    # logger.info(f"Count inside function Before Channhels Creation: {df.count()}")
    
    try:
        df = df.join(maping_combination_df, on='Combination_Id', how='left')
    except Exception as e:
        logger.info(f"Error in Channel merge on combination id")
    # logger.info(f"Count inside function After Channhels Creation: {df.count()}")
    df = df.dropDuplicates(['Header_Id'])
    missing_combination_ids = df.select('OrderSource','OrderType','Combination_Id').filter(col('Combination_Id').isNull()).distinct()
    # Convert DataFrame to dictionary
    df_dict = [row.asDict() for row in missing_combination_ids.collect()]
    if df_dict:
        send_sns_notification(message=f"Hi Mohsin\nHere are The missing combination of orders:\n{df_dict}",subject="Missing Order Combination For DIL", TopicArn=MISSING_COMBINATION_ARN)
    logger.info(f'df_dict {df_dict}')
    logger.info(f"Count inside function after Drop Duplicates Creation: {df.count()}\n {df.columns}")
    df = df.select('Header_Id',"Enriched_Order_Type","Enriched_Channel","Fulfilment_Mode","Combination_Id")
    return df

def add_placeholder_columns(df, columns):
    """
    Add empty columns to a PySpark DataFrame.

    Parameters:
    df (DataFrame): The input PySpark DataFrame.
    columns (list): List of column names to add as empty columns.

    Returns:
    DataFrame: The PySpark DataFrame with added empty columns.
    """
    for col in columns:
        logger.info(f"Adding PlaceHolder Column: {col}")
        df = df.withColumn(col, F.lit(None).cast('string')) # Adding a new column with null values
        
    return df

def fill_null_enriched_order_time(final_header_df):
    logger.info("Fill Null enriched Order Time Called..!")
    logger.info("Count of enriched Order Time balnk Before fill")
    logger.info(final_header_df.filter(col("Enriched_Order_Time").isNull()).count())
    # logger.info(final_header_df.select("Enriched_Order_Time").filter(col("Enriched_Order_Time").isNull().count()))
    # Define a window for sorting (for grouping and ordering nulls)
    enrich_order_time_window = Window.orderBy(
        F.col("InvoiceDate"),
        F.col("StoreCode"),
        F.when(F.col("Enriched_Order_Time").isNull(), 1).otherwise(0),  # Nulls last
        F.col("Enriched_Order_Time")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Fill nulls with the last non-null value
    final_header_df = final_header_df.withColumn(
        'Last_NonNull_Enriched_Time',
        F.last('Enriched_Order_Time', ignorenulls=True).over(enrich_order_time_window)
    )
    
    # Assign row index only for null Enriched_Order_Time values
    null_row_window = Window.partitionBy("InvoiceDate", "StoreCode").orderBy("InvoiceDate", "StoreCode")
    final_header_df = final_header_df.withColumn(
        "null_row_index",
        F.when(F.col("Enriched_Order_Time").isNull(), F.row_number().over(null_row_window)).otherwise(None)
    )

    # Increment null values dynamically by 1 second
    final_header_df = final_header_df.withColumn(
        'Enriched_Order_Time',
        F.when(
            F.col('Enriched_Order_Time').isNull(),
            # Increment by 1 second per row
            F.col('Last_NonNull_Enriched_Time') + F.expr('INTERVAL 1 SECOND') * (F.col('null_row_index') - 1)
        ).otherwise(F.col('Enriched_Order_Time'))
    )

    # Extract only the time portion (HH:mm:ss)
    final_header_df = final_header_df.withColumn('Enriched_Order_Time',F.date_format(F.col('Enriched_Order_Time'), 'HH:mm:ss'))
    
    final_header_df = final_header_df.drop('null_row_index', 'Last_NonNull_Enriched_Time')
    logger.info(f"Count of Enriched_Order_Time Null: {final_header_df.select('Enriched_Order_Time').filter(col('Enriched_Order_Time').isNull()).count()}")
    logger.info(f"Count of Enriched_Order_Time Not Null: {final_header_df.select('Enriched_Order_Time').filter(col('Enriched_Order_Time').isNotNull()).count()}")
    return final_header_df

# udf Definations
get_enriched_webOrderNumber_udf = udf(get_enriched_webOrderNumber, StringType())
convert_to_24hr_format_udf = udf(convert_to_24hr_format, StringType())
get_hour_Day_Part_udf = udf(get_hour_Day_Part, StringType())
# get_Day_Part_from_hour_Day_Part_udf = udf(get_Day_Part_from_hour_Day_Part, StringType())
clean_and_validate_mobile_udf = udf(clean_and_validate_mobile_number, cleaned_phone_number_schema)
week_day_udf = udf(get_week_day, IntegerType())
week_number_udf = udf(get_week_number, IntegerType())
convert_date_format_udf = udf(convert_date_format, StringType())
get_day_part_udf = udf(get_day_part, StringType())