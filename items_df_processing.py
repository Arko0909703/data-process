from config import *
from sns_config import *

items_df_rename_mapping = {
    "OrderId": "Receipt_No",
    "TransactionNo": "Transaction_No",
    "StoreCode": "Store_No",
    "StoreName": "Store_Name",
    "PosTerminalNo": "POS_Terminal_No",
    "Enriched_Order_Date": "Invoice_Open_Date",
    "Enriched_Order_Time": "Invoice_Open_Time",
    'ItemNo': 'Item_No',
    'ItemName': "Item_Name",
    "Promono": "Deal_ID",
    "PromoName": "Deal_Name",
    "LineDiscount": "Line_Discount",
    "Quantity": "Quantity",
    "DiscountAmount": "Discount_Amount",
    "NetAmount": "Line_Net_Amount",
    "CouponNo": "Discount_code",
    "ItemCatagory": "Item_Category",
    "ProductGroup": "Item_Range",
    "TotalRoundedAmount": "Gross",
    "SizeCode": "Size_ID",
    "Counter":"Counter_ID",
    "StandardAmount": "Ala_Carte_Price",
    'TotalDiscount':"Total_Discount",
    "FoodType": "Food_Type",
    "Store_Franchise":"Franchise"}


final_items_columns = ["Line_Id", "Line_No", "Header_Id", "Receipt_No", "Transaction_No",
    "Store_No", "Store_Name", "Counter_ID", "Total_Discount",
    "POS_Terminal_No", "Invoice_Open_Date", "Invoice_Open_Time", "Item_No", "Item_Name",
    "Deal_ID", "Deal_Name", "Line_Discount", "Quantity", "Discount_Amount", "Line_Net_Amount", "Discount_code",
    "Item_Category", "Item_Range", "Gross", "Size_ID", "Ala_Carte_price",
    "Food_Type", "Base", "Parent_ID","Tax_Amount", "Franchise", "Day_Part"]


def rename_items_df(final_items_df, items_df_rename_mapping, final_items_columns):
    # Rename columns in the DataFrame
    for raw_field, final_field in items_df_rename_mapping.items():
        final_items_df = final_items_df.withColumnRenamed(raw_field, final_field)
    return final_items_df.select(*final_items_columns)

def process_items(items_df,final_items_columns):
    try:
        final_items_df = process_item_df(items_df, final_items_columns)
        if final_items_df:
            return final_items_df
        else:
            logger.error("Final Items DataFrame is empty after processing.")
            # send_sns_notification(message="Final Items DataFrame is empty after processing", subject='Response From DIL ETL Script', TopicArn=SNS_TOPIC_ARN)
            return None
    except Exception as e:
        logger.error(f"Error In Items DataFrame Processing:\n{e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # send_sns_notification(message="Error In Items DataFrame Processing", subject='Response From DIL ETL Script', TopicArn=SNS_TOPIC_ARN)
        return None
                            
def process_item_df(items_df, final_items_columns):
    # handler = S3JsonHandler(bucket_name, json_key)
    try:
        final_items_df = items_df.select(
            col("Header_Id"),
            col("OrderId"),
            col("TransactionNo"),
            col('Day_Part'),
            col("Store_Franchise"),
            col("StoreCode"),
            col("StoreName"),
            col("Counter"),
            col("PosTerminalNo"),
            col("Enriched_Order_Date"),
            col("Enriched_Order_Time"),
            col("Valid_Transaction"),
            explode("ItemDetails.Items").alias("Item")
        ).select(col("Header_Id"),
            col("OrderId"),
            col("TransactionNo"),
            col('Day_Part'),
            col("Store_Franchise"),
            col("StoreCode"),
            col("StoreName"),
            col("Counter"),
            col("PosTerminalNo"),
            col("Enriched_Order_Date"),
            col("Enriched_Order_Time"),
            col("Item.CGST").alias("CGST"),
            col("Item.CouponNo").alias("CouponNo"),
            col("Item.DiscountAmount").alias("DiscountAmount"),
            col("Item.FoodType").alias("FoodType"),
            col("Item.ItemCatagory").alias("ItemCatagory"),
            col("Item.ItemName").alias("ItemName"),
            col("Item.ItemNo").alias("ItemNo"),
            col("Item.LineDiscount").alias("LineDiscount"),
            col("Item.NetAmount").alias("NetAmount"),
            col("Item.ProductGroup").alias("ProductGroup"),
            col("Item.PromoName").alias("PromoName"),
            col("Item.Promono").alias("Promono"),
            col("Item.Quantity").alias("Quantity"),
            col("Item.SGST").alias("SGST"),
            col("Item.SizeCode").alias("SizeCode"),
            col("Item.StandardAmount").alias("StandardAmount"),
            col("Item.TotalDiscount").alias("TotalDiscount"),
            col("Item.TotalRoundedAmount").alias("TotalRoundedAmount"))
    except Exception as e:
        logging.error(f'Error in Flattening Items DataFrame: {e}')
        send_sns_notification(f'Error in Flattening Items DataFrame: {e}','Eror In Items Processing')
        raise
    try:
        # Data Type Change
        final_items_df = final_items_df.withColumn("SGST",regexp_replace(col("SGST"), ",", "").cast("double")* -1)
        final_items_df = final_items_df.withColumn("CGST",regexp_replace(col("CGST"), ",", "").cast("double")* -1)
        final_items_df = final_items_df.withColumn("DiscountAmount",regexp_replace(col("DiscountAmount"), ",", "").cast("double")* -1)
        final_items_df = final_items_df.withColumn("Quantity", abs(regexp_replace(col("Quantity"), ",", "").cast("int")))
        final_items_df = final_items_df.withColumn("StandardAmount", regexp_replace(col("StandardAmount"), ",", "").cast("double")* -1)
        final_items_df = final_items_df.withColumn("TotalDiscount",regexp_replace(col("TotalDiscount"), ",", "").cast("double")* -1)
    
        final_items_df = final_items_df.withColumn("LineDiscount",regexp_replace(col("LineDiscount"), ",", "").cast("double")* -1)
        final_items_df = final_items_df.withColumn("TotalRoundedAmount",regexp_replace(col("TotalRoundedAmount"), ",", "").cast("double")* -1)
        # Collect the first 5 entries and convert to list
        # net_amount_list = final_items_df.select("NetAmount").limit(5).collect()

        # # Extract values from Row objects
        # net_amount_values = [row["NetAmount"] for row in net_amount_list]
        # logging.info(f"Net Amount Column Data:\n{net_amount_values}")
        
        if isinstance(final_items_df.schema['NetAmount'].dataType, StringType):
            # logging.info(f'Net Amount Data type is string')
            final_items_df = final_items_df.withColumn("NetAmount", regexp_replace(col("NetAmount"), ",", "").cast("double")* -1)
        elif  isinstance(final_items_df.schema["NetAmount"].dataType, ArrayType):
            # logging.info(f'Net Amount Data type is array')
            final_items_df = final_items_df.withColumn("NetAmount",regexp_replace(col("NetAmount")[0], ",", "").cast("double")* -1)
        elif isinstance(final_items_df.schema["NetAmount"].dataType, StructType):
            # logging.info(f'Net Amount Data type is Struct')
            final_items_df = final_items_df.withColumn("NetAmount",regexp_replace(col("NetAmount")[0], ",", "").cast("double")* -1)
        # Tax Amount
        final_items_df = final_items_df.withColumn("Tax_Amount", (col('SGST')+col('CGST')).cast("double"))
        # Add a new column based on condition
        # final_items_df = final_items_df.withColumn("Base", when((col("ItemCatagory") == "PIZZA") | (col("ItemCatagory") == "FOOD"), col("ProductGroup")).otherwise(lit(None).cast("string")))
        final_items_df = final_items_df.withColumn("Base",when((lower(col("ItemCatagory")) == "pizza") | (lower(col("ItemCatagory")) == "food"),col("ProductGroup")).otherwise(lit(None).cast("string")))
        # Assuming you have a DataFrame named final_payments_df
        window_spec_item_Line_no = Window.partitionBy("OrderId", "StoreCode").orderBy("Enriched_Order_Date")
        # Add a new column with a sequential number partitioned by 'OrderId' and 'StoreCode'
        final_items_df = final_items_df.withColumn("Line_No", F.row_number().over(window_spec_item_Line_no))
        final_items_df = final_items_df.withColumn("Line_Id", concat_ws("_", "Header_Id", "Line_No"))

        # Define window spec for finding the immediate pizza item within the same order
        window_spec_parent_id = Window.partitionBy('Header_Id').orderBy("Line_No").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        # Find the ParentId
        final_items_df = final_items_df.withColumn("Parent_ID", when(col("ItemCatagory") == "TOPPING", last(when(col(
            "ItemCatagory") == "PIZZA", col("ItemNo")), ignorenulls=True).over(window_spec_parent_id)).otherwise(lit(None)))
        
        # Identify columns with NullType
        null_columns = [field.name for field in final_items_df.schema.fields if isinstance(field.dataType, NullType)]

        # Cast NullType columns to StringType
        for column in null_columns:
            final_items_df = final_items_df.withColumn(column, col(column).cast(StringType()))
        # # Replace null values in all columns with an empty string

        fully_processed_item_df = rename_items_df(final_items_df, items_df_rename_mapping, final_items_columns)
        return fully_processed_item_df

    except Exception as e:
        logging.error(f'Error in Items Dataframe Transformation')
        # send_sns_notification(f'Error in Items Dataframe Transformation: {e}','Eror In Items Processing')
        logging.error(traceback.format_exc())
        raise
        