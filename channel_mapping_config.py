from config import *
from custom_loggers import *
import pandas as pd
import pyspark.pandas as ps


dil_channels_sheet = 'dil_combination'
dil_exception_channel_sheet = 'dil_exception_mapping'
channels_mapping_excel_file = 's3://ph-etl-archive/Latest_Without_Multithread/Channles Mapping.xlsx'

def get_dil_order_type_mapping__csv_df(file_path, spark):
    # Use pandas to read the Excel file directly
    df = spark.read.format('csv').option('header', 'true').load(f'{file_path}')
    
    # Fill null values and convert Channel_Id to string
    df = df.withColumn(
        "Channel_Id", 
        F.when(F.col("Channel_Id").isNull(), F.lit(-1))  # Replace nulls with -1
        .otherwise(F.col("Channel_Id").cast('int'))     # Cast to int
    ).withColumn(
        "Channel_Id", F.col("Channel_Id").cast('string')  # Cast to string
    )
    
    return df


def get_dil_order_type_mapping_df_csv(file_path,spark):
    # Use pandas to read the Excel file directly
    df = spark.read.format('csv').option('header', 'true').load(f'{file_path}')
    # Fill null values and convert Channel_Id to string
    # Assuming you have a PySpark DataFrame 'dil_channels'
    df = df.withColumn(
        "Channel_Id", 
        F.when(F.col("Channel_Id").isNull(), F.lit(-1))  # Replace nulls with -1
        .otherwise(F.col("Channel_Id").cast('int'))     # Cast to int
    ).withColumn(
        "Channel_Id", F.col("Channel_Id").cast('string')  # Cast to string
    )
    
    return df


def get_mapping_table(channels_mapping_excel_file,spark, sheet='mapping_table'):
    with pd.option_context('mode.chained_assignment', None):  # Disable chained assignment warning
        # Use pandas directly to avoid pyspark.pandas issues
        pandas_df = pd.read_excel(channels_mapping_excel_file, sheet_name=sheet, engine='openpyxl')
        logger.info(f"Reading {sheet} From S3 For Mapping")

        # Convert pandas DataFrame to PySpark DataFrame
        # mapping_table = ps.from_pandas(pandas_df).to_spark()
        mapping_table = spark.createDataFrame(pandas_df)

    return mapping_table


def get_dil_order_type_mapping_df(sheet, channels_mapping_excel_file):
    with pd.option_context('mode.chained_assignment', None):
        ps_df = ps.read_excel(channels_mapping_excel_file, sheet_name=sheet, engine='openpyxl')[['Sales_Channel_Map', 'Order_Type_Map', 'Channel_Id']]
        logger.info(f"Reading {sheet} From S3 For Mapping")
        ps_df['Channel_Id'] = ps_df['Channel_Id'].fillna(-1).astype('int').astype('string')
        dil_channels = ps_df.to_spark()
        
    return dil_channels

def get_mapping_table(channels_mapping_excel_file, sheet='mapping_table'):
    with pd.option_context('mode.chained_assignment', None):  # Disable chained assignment warning
        ps_df = ps.read_excel(channels_mapping_excel_file, sheet_name=sheet, engine='openpyxl')
        logger.info(f"Reading {sheet} From S3 For Mapping")
        mapping_table = ps_df.to_spark()
        
    return mapping_table

# def get_enriched_order_type(df: DataFrame, channels_df: DataFrame, mapping_table: DataFrame) -> DataFrame:
#     # Check for None DataFrames
#     if df is None or channels_df is None or mapping_table is None:
#         raise ValueError("Input DataFrames cannot be None")

#     # Step 1: Replace nulls with 'Unknown' and trim spaces
#     df = df.withColumn("Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Sales_Channel"))))\
#             .withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Order_Type"))))
#     logger.info("cleaned df for mapping..!")
#     channels_df_cleaned = channels_df.withColumn("Sales_Channel_Map", F.when(F.col("Sales_Channel_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Sales_Channel_Map"))))\
#                                         .withColumn("Order_Type_Map", F.when(F.col("Order_Type_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Order_Type_Map"))))
#     logger.info("cleaned channles Dataframe for mapping..!")
#     # Step 2: Join with channels_df_cleaned
#     final_header_df_mapped = df.join(
#         channels_df_cleaned,
#         (df['Enriched_Sales_Channel'] == channels_df_cleaned['Sales_Channel_Map']) &
#         (df['Enriched_Order_Type'] == channels_df_cleaned['Order_Type_Map']),
#         how='left'
#     )
#     logger.info("Added Channel ID")
#     # Step 3: Revert "Unknown" back to null in Enriched_Sales_Channel and Enriched_Order_Type
#     final_result_df = final_header_df_mapped.withColumn(
#         "Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel") == "Unknown", None).otherwise(F.col("Enriched_Sales_Channel"))
#     ).withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type") == "Unknown", None).otherwise(F.col("Enriched_Order_Type")))
    
#     # Step 4: Join with mapping_table on Channel_Id
#     final_header_df_mapped = final_result_df.join(
#         mapping_table,
#         final_result_df["Channel_Id"] == mapping_table["Mapping_Channel_Id"],
#         how='left'
#     ).drop(mapping_table["Mapping_Channel_Id"])
#     logger.info("Added Enriched Channles")
    
#     return final_header_df_mapped


def get_enriched_order_type(df: DataFrame, channels_df: DataFrame, mapping_table: DataFrame) -> DataFrame:
    # Check for None DataFrames
    if df is None or channels_df is None or mapping_table is None:
        raise ValueError("Input DataFrames cannot be None")
    if "Channel_Id" not in df.columns:
        logger.info("Processing Normal channels")
        df, channels_df_cleaned = get_df_and_channels_cleaned(df,channels_df)
        # Step 2: Join with channels_df_cleaned
        final_header_df_mapped = df.join(
            broadcast(channels_df_cleaned),
            (lower(df['Enriched_Sales_Channel']) == lower(channels_df_cleaned['Sales_Channel_Map'])) &
            (lower(df['Enriched_Order_Type']) == lower(channels_df_cleaned['Order_Type_Map'])),
            how='left'
        )
        logger.info("Added Channel ID")
        # Step 3: Revert "Unknown" back to null in Enriched_Sales_Channel and Enriched_Order_Type
        final_result_df = final_header_df_mapped.withColumn(
            "Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel") == "Unknown", None).otherwise(F.col("Enriched_Sales_Channel"))
        ).withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type") == "Unknown", None).otherwise(F.col("Enriched_Order_Type")))

        # Step 4: Join with mapping_table on Channel_Id
        final_header_df_mapped = final_result_df.join(
            mapping_table,
            final_result_df["Channel_Id"] == mapping_table["Mapping_Channel_Id"],
            how='left'
        )
        final_header_df_mapped =  final_header_df_mapped.drop('Mapping_Channel_Id','Sales_Channel_Map', 'Order_Type_Map')
        logger.info("Added Enriched Channles")

        return final_header_df_mapped
    elif "Channel_Id" in df.columns:
        logger.info(f"Proceesing Exceptions Channels")
        df = df.withColumnRenamed("Source", "Old_Source")\
                .withColumnRenamed("Channel", "Old_Channel")\
                .withColumnRenamed("Fulfilment_Mode", "Old_Fulfilment_Mode")\
                .withColumnRenamed("Channel_Id", "Old_Channel_Id")
        old_columns = ['Source','Channel','Fulfilment_Mode']
        # for col in old_columns:
        #     logger.info(f"Renaming {col} ---> old_{col}")
        # df = df.withColumn("Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Sales_Channel"))))\
        #         .withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Order_Type"))))
        # logger.info("cleaned df for mapping..!")
        # channels_df_cleaned = channels_df.withColumn("Sales_Channel_Map", F.when(F.col("Sales_Channel_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Sales_Channel_Map"))))\
        #                                 .withColumn("Order_Type_Map", F.when(F.col("Order_Type_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Order_Type_Map"))))
        df, channels_df_cleaned = get_df_and_channels_cleaned(df,channels_df)
        logger.info("cleaned channles Dataframe for mapping..!")
        # Step 2: Join with channels_df_cleaned
        final_header_df_mapped = df.join(
            broadcast(channels_df_cleaned),
            (lower(df['Enriched_Sales_Channel']) == lower(channels_df_cleaned['Sales_Channel_Map'])) &
            (lower(df['Enriched_Order_Type']) == lower(channels_df_cleaned['Order_Type_Map'])),
            how='left'
        )
        logger.info("Added Channel ID")
        # Step 3: Revert "Unknown" back to null in Enriched_Sales_Channel and Enriched_Order_Type
        final_result_df = final_header_df_mapped.withColumn(
            "Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel") == "Unknown", None).otherwise(F.col("Enriched_Sales_Channel"))
        ).withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type") == "Unknown", None).otherwise(F.col("Enriched_Order_Type")))

        # Step 4: Join with mapping_table on Channel_Id
        final_header_df_mapped = final_result_df.join(
            broadcast(mapping_table),
            final_result_df["Channel_Id"] == mapping_table["Mapping_Channel_Id"],
            how='left'
        )
        final_header_df_mapped =  final_header_df_mapped.drop('Mapping_Channel_Id','Sales_Channel_Map', 'Order_Type_Map')
        logger.info("Added Enriched Channles")

        return final_header_df_mapped

def add_exception_channels_to_df_with_weborder_no(df:DataFrame, dil_channels_exceptions:DataFrame, mapping_table:DataFrame) -> DataFrame:
    # logger.info("Inside Channles Overwrite\nSeperating with and Without Weborder Number DF")
    df_with_webOrder_number= df.filter(col('WeborderNo').isNotNull())
    df_without_webOrder_number = df.filter(col('WeborderNo').isNull())
    # mapped_webOrder_number = mapped_webOrder_number.drop("Channel_Id")
    new_mapped_df_with_webOrder_number = get_enriched_order_type(df_with_webOrder_number,dil_channels_exceptions,mapping_table)
    final_mapped_df = new_mapped_df_with_webOrder_number.withColumn("Channel_Id",F.coalesce("Old_Channel_Id", "Channel_Id"))\
                .withColumn("Enriched_Order_Type",F.coalesce("Old_Source", "Enriched_Order_Type"))\
                .withColumn("Enriched_Channel",F.coalesce("Channel", "Enriched_Channel"))\
                .withColumn("Fulfilment_Mode",F.coalesce("Old_Fulfilment_Mode", "Fulfilment_Mode"))\
                .drop("Old_Source")\
                .drop("Channel")\
                .drop("Old_Fulfilment_Mode")\
                .drop("Old_Channel_Id")\
                .drop("Channel_Id")\
                .drop("Mapping_Channel_Id")\
                .drop('Sales_Channel_Map')\
                .drop("Order_Type_Map")
    
    if not df_without_webOrder_number.isEmpty():
        final_channels_mapped_df = final_mapped_df.unionByName(df_without_webOrder_number,allowMissingColumns=True)
    else:
        final_channels_mapped_df = df_with_webOrder_number
    logger.info("ADDED EXCEPTIONS MAPPING..!")
    return final_channels_mapped_df

def get_df_and_channels_cleaned(df:DataFrame, channels_df:DataFrame):
    """
    Cleans the input DataFrame and channels DataFrame by replacing null values with 'Unknown' 
    and trimming spaces from specified columns.

    Parameters:
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing order details with columns such as 'Enriched_Sales_Channel' and 'Enriched_Order_Type'.
        
    channels_df : pyspark.sql.DataFrame
        The DataFrame containing channel mappings with columns such as 'Sales_Channel_Map' and 'Order_Type_Map'.

    Returns:
    -------
    tuple of pyspark.sql.DataFrame
        - The cleaned version of the input DataFrame 'df', where 'Enriched_Sales_Channel' and 'Enriched_Order_Type' 
            have nulls replaced with 'Unknown' and are trimmed.
        - The cleaned version of 'channels_df', where 'Sales_Channel_Map' and 'Order_Type_Map' 
            have nulls replaced with 'Unknown' and are trimmed.

    Logs:
    ----
    - Logs a message indicating that null values in 'Enriched_Sales_Channel' and 'Enriched_Order_Type' have been replaced with 'Unknown'.
    - Logs a message indicating that null values in 'Sales_Channel_Map' and 'Order_Type_Map' 
        have been replaced with 'Unknown'.
    
    Example:
    -------
    df_cleaned, channels_cleaned = get_df_and_channels_cleaned(order_df, channels_mapping_df)
    """
    # Step 1: Replace nulls with 'Unknown' and trim spaces
    df = df.withColumn("Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Sales_Channel"))))\
            .withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Order_Type"))))
    logger.info("cleaned df for mapping..! i.e Replaced Null with UNKNOWN")
    channels_cleaned = channels_df.withColumn("Sales_Channel_Map", F.when(F.col("Sales_Channel_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Sales_Channel_Map"))))\
                                        .withColumn("Order_Type_Map", F.when(F.col("Order_Type_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Order_Type_Map"))))
                                        
    logger.info("cleaned Channels Df for mapping..! i.e Replaced Null with UNKNOWN")
    return df, channels_cleaned