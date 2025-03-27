from config import *
from custom_loggers import *
import pandas as pd
import pyspark.pandas as ps


dil_channels_sheet = 'dil_combination'
dil_exception_channel_sheet = 'dil_exception_mapping'
channels_mapping_excel_file = 's3://ph-etl-archive/DIL_TEST_PROD_DATA_NORMALISATION/Latest_Without_Multithread/Channel_Master.xlsx'


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

def get_enriched_order_type(df: DataFrame, channels_df: DataFrame, mapping_table: DataFrame) -> DataFrame:
    # Check for None DataFrames
    if df is None or channels_df is None or mapping_table is None:
        raise ValueError("Input DataFrames cannot be None")
    if "Channel_Id" not in df.columns:
        print("Processing Normal channels")
        # Step 1: Replace nulls with 'Unknown' and trim spaces
        df = df.withColumn("Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Sales_Channel"))))\
                .withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Order_Type"))))
        print("cleaned df for mapping..!")
        channels_df_cleaned = channels_df.withColumn("Sales_Channel_Map", F.when(F.col("Sales_Channel_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Sales_Channel_Map"))))\
                                            .withColumn("Order_Type_Map", F.when(F.col("Order_Type_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Order_Type_Map"))))
        print("cleaned channles Dataframe for mapping..!")
        # Step 2: Join with channels_df_cleaned
        final_header_df_mapped = df.join(
            channels_df_cleaned,
            (df['Enriched_Sales_Channel'] == channels_df_cleaned['Sales_Channel_Map']) &
            (df['Enriched_Order_Type'] == channels_df_cleaned['Order_Type_Map']),
            how='left'
        )
        print("Added Channel ID")
        # Step 3: Revert "Unknown" back to null in OrderSource and OrderType
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
        print("Added Enriched Channles")

        return final_header_df_mapped
    elif "Channel_Id" in df.columns:
        print(f"Proceesing Exceptions Channels")
        df = df.withColumnRenamed("Source", "Old_Source")\
                .withColumnRenamed("Channel", "Old_Channel")\
                .withColumnRenamed("Fulfilment_Mode", "Old_Fulfilment_Mode")\
                .withColumnRenamed("Channel_Id", "Old_Channel_Id")
        # print("Enriched_Order_Type ---> Old_Enriched_Order_Type")
        df = df.withColumn("Enriched_Sales_Channel", F.when(F.col("Enriched_Sales_Channel").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Sales_Channel"))))\
                .withColumn("Enriched_Order_Type", F.when(F.col("Enriched_Order_Type").isNull(), "Unknown").otherwise(F.trim(F.col("Enriched_Order_Type"))))
        print("cleaned df for mapping..!")
        channels_df_cleaned = channels_df.withColumn("Sales_Channel_Map", F.when(F.col("Sales_Channel_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Sales_Channel_Map"))))\
                                         .withColumn("Order_Type_Map", F.when(F.col("Order_Type_Map").isNull(), "Unknown").otherwise(F.trim(F.col("Order_Type_Map"))))
        
        print("cleaned channles Dataframe for mapping..!")
        # Step 2: Join with channels_df_cleaned
        final_header_df_mapped = df.join(
            channels_df_cleaned,
            (df['Enriched_Sales_Channel'] == channels_df_cleaned['Sales_Channel_Map']) &
            (df['Enriched_Order_Type'] == channels_df_cleaned['Order_Type_Map']),
            how='left'
        )
        print("Added Channel ID")
        # Step 3: Revert "Unknown" back to null in OrderSource and OrderType
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
        print("Added Enriched Channles")

        return final_header_df_mapped
def add_exception_channels_to_df_with_weborder_no(df, dil_channels_exceptions, mapping_table):
    print("Inside Channles Overwrite\nSeperating with and Without Weborder Number DF")
    print(f"Total Data Count: {df.count()}")
    mapped_webOrder_number = df.filter(col('WeborderNo').isNotNull())
    print(f"mapped_webOrder_number count: {mapped_webOrder_number.count()}\n")
    
    mapped_without_webOrder_number = df.filter(col('WeborderNo').isNull())
    print(f"mapped_without_webOrder_number count: {mapped_without_webOrder_number.count()}\n")
    
    # mapped_webOrder_number = mapped_webOrder_number.drop("Channel_Id")
    # print(mapped_webOrder_number.columns)
    new_mapped_df_with_webOrder_number = get_enriched_order_type(mapped_webOrder_number,dil_channels_exceptions,mapping_table)
    # print(f"new_mapped_df_with_webOrder_number sample: {new_mapped_df_with_webOrder_number.head(10)}")
    # filtered_df = new_mapped_df_with_webOrder_number.select("Source", "Channel", "Fulfilment_Mode", "Channel_Id").filter(col("Channel_Id").isNull())
    # print(f"filtered_df count: {filtered_df.count()}\n")
    
    final_mapped_df = new_mapped_df_with_webOrder_number.withColumn("Channel_Id",F.coalesce("Channel_Id","Old_Channel_Id"))\
                .withColumn("Source",F.coalesce("Source", "Old_Source" ))\
                .withColumn("Channel",F.coalesce("Channel", "Old_Channel"))\
                .withColumn("Fulfilment_Mode",F.coalesce("Fulfilment_Mode","Old_Fulfilment_Mode"))\
                .drop("Old_Enriched_Order_Type")\
                .drop("Old_Enriched_Channel")\
                .drop("Old_Fulfilment_Mode")\
                .drop("Old_Channel_Id")\
                .drop("Mapping_Channel_Id")\
                .drop('Sales_Channel_Map')\
                .drop("Order_Type_Map")
    
    # final_mapped_df.select("Enriched_Order_Type", "Enriched_Channel", "Fulfilment_Mode", "Channel_Id").filter(col("Channel_Id").isNull()).show()
    if not mapped_without_webOrder_number.isEmpty():
        mapped_final_header_df = final_mapped_df.unionByName(mapped_without_webOrder_number,allowMissingColumns=True)
    else:
        mapped_final_header_df = mapped_webOrder_number
    print(f"Output Dataframe Count:{mapped_final_header_df.count()}")
    print("ADDED EXCEPTIONS MAPPING..!")
    return mapped_final_header_df