# Import necessary modules and libraries
import sys
import os
import re
import logging
import traceback
from io import BytesIO, StringIO
import pandas as pd
from datetime import datetime, timedelta,time
from typing import Optional, List, Tuple, Dict, Union, Generator
from dateutil import parser


from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# PySpark imports
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    ArrayType,
    BooleanType,
    DateType,
    NullType
)
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lower,
    explode,
    last,
    array,
    row_number,
    format_string,
    udf,
    monotonically_increasing_id,
    regexp_replace,
    expr,
    concat_ws,
    current_date,
    date_format,
    abs,
    from_unixtime,
    unix_timestamp,
    when,
    to_date,
    dayofweek,
    weekofyear,
    trim,
    count,
    coalesce,
    broadcast,
    lit,
    to_timestamp,
    year,
    month,
    collect_list,
    sum,
    array_contains,
    pandas_udf
)
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import pyspark.pandas as ps

# AWS and Encryption libraries
import boto3
import json
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from pyspark.sql.functions import broadcast

# Concurrency libraries
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing
from threading import Lock

# Custom logger setup
from custom_loggers import setup_in_memory_logging

# Initialize logging
logger, log_stream = setup_in_memory_logging()

# Paths and file configurations
maping_combination_csv_file = 's3://ph-etl-archive/Data Normalisation DIL/Channel_combinations/channel_mapping_channel_combination.csv'
order_combination_csv_file = 's3://ph-etl-archive/Data Normalisation DIL/Channel_combinations/channel_mapping_order_combination.csv'
excel_file_path = 's3://ph-etl-archive/Data Normalisation DIL/Channles Mapping.xlsx'
# data_save_s3_path = 's3://ph-etl-archive/Dil_Data_Normalised_Data/'
invalid_numbers_file_path = 's3://ph-etl-archive/pravin-testing-etl/Configureations/invalid_numbers.txt'

# AWS S3 and SNS configurations
INPUT_DATA_BUCKET = 'ph-etl-wip'
OUTPUT_DATA_BUCKET = 'ph-etl-archive'


s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Data processing configurations
frenchise = 'dil'
PHDV_PROCESSING_DAYS = 2
PHDV_FOLDER = 'phdv-data'
PHDV_BUCKET_NAME = 'ph-etl-archive'

# Schema and DataFrame configurations
header_df_columns = [
    'BillCloseddate', 'BillClosetime', 'CGST', 'City', 'Counter', 'CouponNo', 
    'CustomerName', 'Email', 'EntryStatus', 'InvoiceDate', 'InvoiceNo', 
    'InvoiceTime', 'NetAmount', 'OrderId', 'OrderSource', 'OrderType', 
    'OriginatedFromStore', 'PhoneNo', 'PosTerminalNo', 'ReteriveFromReceiptNo', 
    'SGST', 'SaleIsReturn', 'StoreCode', 'StoreName', 'TotalOrderValue', 
    'Totaldiscount', 'Totalorderqty', 'TransactionNo', 'WeborderNo'
]

STORE_MASTER_COLUMNS = [
    'FZE', 'Champs', 'City', 'State', 'Zone', 'Cluster', 'Format', 'Location', 
    'Tier', 'Region', 'updated_code','S_Name'
]

# Register UDF
cleaned_phone_number_schema = StructType([
    StructField('cleaned_mobile_number', StringType(), True),
    StructField('validation_tag', BooleanType(), True)])



# Define the schema
RAW_XML_DATA_SCHEMA = StructType([
    StructField('BillCloseddate', StringType(), True),
    StructField('BillClosetime', StringType(), True),
    StructField('CGST', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Counter', StringType(), True),  
    StructField('CouponNo', StringType(), True),
    StructField('CustomerName', StringType(), True),
    StructField('Email', StringType(), True),
    StructField('EntryStatus', StringType(), True),
    StructField('InvoiceDate', StringType(), True),
    StructField('InvoiceNo', StringType(), True),
    StructField('InvoiceTime', StringType(), True),
    StructField('ItemDetails', StructType([
        StructField('Items', ArrayType(
            StructType([
                StructField('CGST', StringType(), True),
                StructField('CouponNo', StringType(), True),
                StructField('DiscountAmount', StringType(), True),
                StructField('FoodType', StringType(), True),
                StructField('ItemCatagory', StringType(), True),
                StructField('ItemName', StringType(), True),
                StructField('ItemNo', StringType(), True),
                StructField('LineDiscount', StringType(),True),
                StructField('NetAmount', StringType(), True),
                StructField('ProductGroup', StringType(), True),
                StructField('PromoName', StringType(), True),
                StructField('Promono', StringType(), True),
                StructField('Quantity', StringType(), True),
                StructField('SGST', StringType(), True),
                StructField('SizeCode', StringType(), True),
                StructField('StandardAmount', StringType(), True),
                StructField('TotalDiscount', StringType(), True),
                StructField('TotalRoundedAmount', StringType(), True)
            ])
        ), True)
    ]), True),
    StructField('NetAmount', StringType(), True),
    StructField('OrderId', StringType(), True),
    StructField('OrderSource', StringType(), True),
    StructField('OrderType', StringType(), True),
    StructField('OriginatedFromStore', StringType(), True),
    StructField('PaymentsDetails', StructType([
        StructField('Payments', ArrayType(
            StructType([
                StructField('Amount', StringType(), True),
                StructField('Date', StringType(), True),
                StructField('Mode', StringType(), True),
                StructField('TenderType', StringType(), True)
            ])
        ), True)
    ]), True),
    StructField('PhoneNo', StringType(), True),  
    StructField('PosTerminalNo', StringType(), True),
    StructField('ReteriveFromReceiptNo', StringType(), True),
    StructField('SGST', StringType(), True),
    StructField('SaleIsReturn', StringType(), True),
    StructField('StoreCode', StringType(), True),
    StructField('StoreName', StringType(), True),
    StructField('TotalOrderValue', StringType(), True),
    StructField('Totaldiscount', StringType(), True),  
    StructField('Totalorderqty', StringType(), True),  
    StructField('TransactionNo', StringType(), True),  
    StructField('WeborderNo', StringType(), True)
])


RAW_RAW_PHDV_SCHEMA = StructType([
    StructField('Latitude', StringType(), nullable=True),
    StructField('Store Code', StringType(), nullable=True),
    StructField('Longitude', StringType(), nullable=True),
    StructField('OrderNo', StringType(), nullable=True),
    StructField('marketing Communication', StringType(), nullable=True),
    StructField('Order_Date', StringType(), nullable=True),
    StructField('Order Time', StringType(), nullable=True),
    StructField('Pasta by PH', StringType(), nullable=True),
    StructField('Discount Voucher Code', StringType(), nullable=True),
    StructField('Discount Amount', StringType(), nullable=True),
    StructField('discount_code', StringType(), nullable=True),
    StructField('Discount_type_ID', StringType(), nullable=True),
    StructField('Discount_type_type', StringType(), nullable=True),
    StructField('Discount_appexclusive', StringType(), nullable=True),
    StructField('Discount_value', StringType(), nullable=True),
    StructField('Aggregator Order ID', StringType(), nullable=True),
    StructField('MobileNo', StringType(), nullable=True),
    StructField('Email', StringType(), nullable=True),
    StructField('Order Status', StringType(), nullable=True),
    StructField('Order Type', StringType(), nullable=True),
    StructField('New_Channel', StringType(), nullable=True),
    StructField('Reference No', StringType(), nullable=True),
    StructField('Gift Card', StringType(), nullable=True),
    StructField('Gift card amount', StringType(), nullable=True),
    StructField('Gift card No', StringType(), nullable=True),
    StructField('utm_source', StringType(), nullable=True),
    StructField('utm_medium', StringType(), nullable=True),
    StructField('utm_campaign', StringType(), nullable=True),
    StructField('Franchise', StringType(), nullable=True)
])

final_header_df_rename_mapping = {
    'OrderId': 'Receipt_No',
    'TransactionNo': 'Transaction_No',
    'InvoiceNo': 'Invoice_No',
    'WeborderNo': 'WebOrderNo',
    'StoreCode': 'Store_No',
    'City': 'Store_City',
    'StoreName': 'Store_Name',
    'OrderSource': 'Sales_Channel',
    'OrderType': 'Order_Type',
    'PosTerminalNo': 'POS_Terminal_No',
    'SaleIsReturn': 'Sale_Is_Return_Sale',
    'ReteriveFromReceiptNo': 'Retrieved_Receipt_No',
    'InvoiceDate': 'Invoice_Open_Date',
    'InvoiceTime': 'Invoice_Open_Time',
    'BillCloseddate': 'Invoice_Closed_Date',
    'BillClosetime': 'Invoice_closed_Time',
    'CustomerName': 'Customer_Name',
    'Email': 'Email',
    'Totalorderqty': 'Order_Quantity',
    'TotalOrderValue': 'Gross_Amount',
    'NetAmount': 'Net_Amount',
    'SGST': 'SGST',
    'CGST': 'CGST',
    'Totaldiscount': 'Discount_Amount',
    'CouponNo': 'Coupon_Code',
    'Counter': 'Counter_ID',
    'EntryStatus': 'Entry_Status',
    'OriginatedFromStore': 'Originated_From_Store',
    'Latitude': 'PHDV_Latitude',
    'Longitude': 'PHDV_Longitude',
    'marketing Communication': 'PHDV_Marketing_Communication',
    'Discount Voucher Code': 'PHDV_Discount_Voucher_Code',
    'discount_code': 'PHDV_Discount_Code',
    'Discount_type_ID': 'PHDV_Discount_Type_Id',
    'Discount_type_type': 'PHDV_Discount_Type',
    'Discount_appexclusive': 'PHDV_Discount_App_Exclusive',
    'Discount_value': 'PHDV_Discount_Value',
    'Aggregator Order ID': 'PHDV_Aggregator_Id',
    'Order Status': 'PHDV_Order_Status',
    'Order Type': 'PHDV_OrderType',
    'New_Channel': 'PHDV_Channel',
    'Reference No': 'PHDV_Reference_No',
    'Gift Card': 'PHDV_GiftCard',
    'Gift card amount': 'PHDV_GiftCard_Amount',
    'Gift card No': 'PHDV_GiftCard_No',
    'Order Time':'PHDV_Order_Time',
    'utm_source': 'PHDV_Utm_Source',
    'utm_medium': 'PHDV_Utm_Medium',
    'utm_campaign': 'PHDV_Utm_Campaign',
    'Discount Amount': 'PHDV_Discount_Amount',
    'CleanedPhoneNumber':'Mobile_Number',
    'Enriched_WebOrder_No':'Enriched_WebOrderNo',
    'Pizzahut_Brand':'PHDV_Pizzahut_Brand'}

baseline_columns = ['Header_Id','Franchise','Receipt_No','Store_No', 'Invoice_Open_Date', 
                    'Enriched_Order_Date','Invoice_Open_Time', 'Enriched_Order_Time',
                    'Sales_Channel','Order_Type','Enriched_Sales_Channel',
                    'Enriched_Order_Type', 'Channel_Id','Source','Channel','Fulfilment_Mode',
                    'Order_Quantity','Net_Amount', 'Gross_Amount','Tax_Amount','SGST','CGST',
                    'Discount_Amount','Coupon_Code','Payment_Mode','Gift_Card','Gift_Card_Amount',
                    'Deal','Ala_Carte','Non_Veg','Year','Month','Week_Day','Week_Number','Hour_Day_Part','Day_Part',
                    'WebOrderNo','Enriched_WebOrderNo','PHDV_Reference_No','PHDV_OrderType','PHDV_Channel',
                    'PHDV_Pizzahut_Brand','PHDV_Latitude','PHDV_Longitude','PHDV_Marketing_Communication',
                    'PHDV_Discount_Voucher_Code','PHDV_Discount_Amount','PHDV_Discount_Code',
                    'PHDV_Discount_Type_Id','PHDV_Discount_Type','PHDV_Discount_App_Exclusive',
                    'PHDV_Discount_Value','PHDV_Aggregator_Id','PHDV_Order_Status','PHDV_GiftCard',
                    'PHDV_Order_Time', 'PHDV_GiftCard_Amount','PHDV_GiftCard_No','PHDV_Utm_Source',
                    'PHDV_Utm_Medium','PHDV_Utm_Campaign','Orderwise_Id','Transaction_No', 'Invoice_No','POS_Terminal_No',
                    'Sale_Is_Return_Sale','Retrieved_Receipt_No','Business_Date', 'Invoice_Closed_Date','Invoice_closed_Time',
                    'Counter_ID','Originated_From_Store','Store_Name','Store_City',
                    'StoreMaster_Franchise', 'StoreMaster_Champ_Id', 'StoreMaster_Store_City', 'StoreMaster_State',
                    'StoreMaster_Cluster', 'StoreMaster_Format','StoreMaster_Location', 'StoreMaster_Tier_Category',
                    'StoreMaster_Region', 'StoreMaster_Zone','System_Processed_Date','Entry_Status', 'Valid_Transaction']

# baseline_columns = ['Header_Id','Orderwise_Id','Receipt_No','Transaction_No',
#                           'Invoice_No','WebOrderNo',
#                           'Store_No','Store_Name',
#                           'Store_City','Sales_Channel',
#                           'Order_Type','POS_Terminal_No',
#                           'Sale_Is_Return_Sale','Retrieved_Receipt_No',
#                           'Invoice_Open_Date','Invoice_Open_Time',
#                           'Invoice_Closed_Date','Invoice_closed_Time',
#                           'Business_Date','Order_Quantity',
#                           'Gross_Amount','Net_Amount','Tax_Amount','SGST','CGST',
#                           'Discount_Amount','Coupon_Code',
#                           'Counter_ID','Entry_Status','Originated_From_Store','System_Processed_Date',
#                           'Day_Part','PHDV_Latitude','PHDV_Longitude',
#                           'PHDV_Marketing_Communication','Enriched_Order_Date',
#                           'Enriched_Order_Time','Hour_Day_Part',
#                           'Year','Month','Enriched_Sales_Channel','Enriched_Order_Type','Source','Channel',
#                           'Fulfilment_Mode','Channel_Id','Enriched_WebOrder_No','Week_Day',
#                           'Week_Number','Pizzahut_Brand','Valid_Transaction','Deal','Ala_Carte','Franchise',
#                           'StoreMaster_Franchise', 'StoreMaster_Champ_Id', 'StoreMaster_Store_City', 'StoreMaster_State', 
#                           'StoreMaster_Cluster', 'StoreMaster_Format','StoreMaster_Location', 'StoreMaster_Tier_Category',
#                           'StoreMaster_Region', 'StoreMaster_Zone','Payment_Mode','Gift_Card','Gift_Card_Amount',
#                           'Non_Veg','PHDV_Discount_Voucher_Code','PHDV_Discount_Amount','PHDV_Discount_Code',
#                           'PHDV_Discount_Type_Id','PHDV_Discount_Type','PHDV_Discount_App_Exclusive',
#                           'PHDV_Discount_Value','PHDV_Aggregator_Id','PHDV_Order_Status','PHDV_OrderType',
#                           'PHDV_Channel','PHDV_Reference_No','PHDV_GiftCard','PHDV_Order_Time', 'PHDV_GiftCard_Amount',
#                           'PHDV_GiftCard_No','PHDV_Utm_Source','PHDV_Utm_Medium','PHDV_Utm_Campaign']

mapping_dataframe_columns = ['Header_Id',   'Store_No',
                         'Channel', 'Source',
                         'Fulfilment_Mode',    'PHDV_Aggregator_Id',
                         'PHDV_Reference_No',    'Enriched_WebOrderNo',
                         'Receipt_No',    'Transaction_No',
                         'Invoice_No',    'Enriched_Order_Date',
                         'Enriched_order_Time', 'Franchise', 'PHDV_Pizzahut_Brand', 'Mobile_Number']

pii_df_columns = ['Mobile_Number', 'Valid', 'Header_Id', 'Customer_Name', 'Enriched_Email_Id', 
                  'Enriched_Order_Date','Enriched_Order_Time', 'Store_No', 'Receipt_No', 'PHDV_Latitude', 
                  'PHDV_Longitude', 'Store_City', 'Franchise']

header_df_mandatory_data_columns = ['OrderId', 'TransactionNo', 'StoreCode','InvoiceDate', 'Totalorderqty', 'TotalOrderValue', 'NetAmount', 'SGST', 'CGST']
items_df_mandatory_data_columns = ['OrderId', 'TransactionNo','StoreCode', 'ItemNo', 'Quantity', 'NetAmount', 'TotalRoundedAmount']
payment_df_mandatory_data_columns = ['OrderId', 'TransactionNo', 'StoreCode', 'PosTerminalNo', 'TenderType', 'Amount']

payment_df_columns = ['Header_Id','OrderId','TransactionNo','StoreCode','StoreName','BillClosetime',
                      'PosTerminalNo','Totalorderqty','Enriched_Order_Date', 'Enriched_Order_Time','PaymentsDetails']

item_df_columns = ['Header_Id', 'OrderId', 'TransactionNo', 'StoreCode', 'Day_Part','Store_Franchise','StoreName', 
                   'Counter', 'PosTerminalNo', 'Enriched_Order_Date', 'Enriched_Order_Time','Valid_Transaction', 'ItemDetails']
