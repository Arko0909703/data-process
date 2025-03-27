from config import *


def get_invalid_phone_numbers_list(bucket_name='ph-etl-archive', file_key ='Latest_Without_Multithread/invalid_phone_numbers.txt'):
    """
    Reads a text file from S3 and returns a list where each element is a line from the file.
    
    Parameters:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key (path) of the file in the S3 bucket.
    
    Returns:
        list: A list where each element is a line from the text file.
    """
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Read the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')
    print(file_content)
    print(type(file_content))
    text_list = [item.strip() for item in file_content.split(',')]
    print(type(text_list), len(text_list))
    
    return text_list
def clean_phone_number(mobile_number):
    if mobile_number is not None:
        # Remove spaces
        mobile_number = mobile_number.replace(" ", "").strip()

        # Condition 2: Pick MobileNo of length 11
        if len(mobile_number) == 11 and mobile_number[0] == '0':
            return mobile_number[1:]  # Remove leading 0

        elif len(mobile_number) < 10:
            # print(f'Lenghth Of Mobile :{len(mobile_number)}')
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
        # print(f"Cleaned Number: {cleaned_number}")

        # Condition 7: Check if transactions today exceed 4
        if transaction_count >= 4:
            clean_number = cleaned_number
            validation_tag = False

        # Check if cleaned_number is in invalid_numbers list
        # invalid_phone_numbers_list = ['9999999999', '8888888888','7777777777', '6666666666', '9876543210']
        invalid_phone_numbers_list = get_invalid_phone_numbers_list()
        if clean_number in invalid_phone_numbers_list:
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


# Register UDF
cleaned_phone_number_schema = StructType([
    StructField("cleaned_mobile_number", StringType(), True),
    StructField("validation_tag", BooleanType(), True)
])

clean_and_validate_mobile_udf = udf(
    clean_and_validate_mobile_number, cleaned_phone_number_schema)

def get_phone_number_processed(phone_number_df):
    phone_number_df = phone_number_df.withColumn("CombinedNumber", coalesce(col("MobileNo"), col("PhoneNo")))
    # Define a window specification to partition by CombinedNumber and InvoiceDate
    window_spec = Window.partitionBy("CombinedNumber", "InvoiceDate")
    # Step 1: Add a new column 'transactions_today' based on count within the window which tracks number of transaction
    phone_number_df = phone_number_df.withColumn("transactions_today", F.count("*").over(window_spec))
    # Step 2: Apply UDF to create cleaned_mobile_number and validation_tag columns
    phone_number_df = phone_number_df.withColumn("cleaned_mobile_data",clean_and_validate_mobile_udf(col("CombinedNumber"), col("transactions_today")))
    # Step 3: Flatten the struct column into individual columns
    phone_number_df = phone_number_df.withColumn("CleanedPhoneNumber", col("cleaned_mobile_data.cleaned_mobile_number")).withColumn("Valid", col("cleaned_mobile_data.validation_tag"))
    # Replace null values in PhoneNo column with 'xxxxxxxxxx'
    phone_number_df = phone_number_df.na.fill({'CleanedPhoneNumber': 'xxxxxxxxxx'})
    # phone_number_df = phone_number_df.dropDuplicates()
    phone_number_df = phone_number_df.select(['Header_Id', 'CombinedNumber', 'transactions_today','CleanedPhoneNumber', 'Valid'])
    logger.info("Processed Phone number df and returning")
    return phone_number_df

clean_and_validate_mobile_udf = udf(clean_and_validate_mobile_number, cleaned_phone_number_schema)