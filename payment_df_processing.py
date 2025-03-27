from config import *

payment_df_rename_mapping = {
    "OrderId": "Receipt_No",
    "TransactionNo": "Transaction_No",
    "StoreCode": "Store_No",
    "StoreName": "Store_Name",
    "PosTerminalNo": "POS_Terminal_No",
    "Enriched_Order_Date": "Invoice_Open_Date",
    "Enriched_Order_Time":"Invoice_Open_Time",
    "BillClosetime": "Tender_Time",
    "Payment_Date": "Tender_Date",
    "Payment_TenderType": "Tender_No",
    "Payment_Mode": "Tender_Name",
    "Payment_Amount": "Gross"}

def rename_payment_df(final_payments_df,payment_df_rename_mapping ):
    # Rename columns in the DataFrame
    for raw_field, final_field in payment_df_rename_mapping.items():
        final_payments_df = final_payments_df.withColumnRenamed(raw_field, final_field)
    return final_payments_df.select('Payment_Id','Line_No','Header_Id','Receipt_No','Transaction_No','Store_No','Store_Name','POS_Terminal_No','Invoice_Open_Date','Invoice_Open_Time','Tender_date','Tender_time','Tender_No','Tender_Name','Gross')

def process_payment(payment_df,payment_df_rename_mapping):
    try:
        final_payments_df, gift_card_info_df = process_payment_df(payment_df, payment_df_rename_mapping)
        # if not gift_card_info_df.isEmpty() and not final_payments_df.isEmpty():
        #     return final_payments_df, gift_card_info_df
        #     logger.info("gift_card_info_df and final_payments_df Available")
        # else:
        #     logger.error("No Gift Card information DataFrame Available.")
        #     send_sns_notification(message="No Gift Card information DataFrame Available From PaymentsDetails", subject='Response From DIL ETL Script', TopicArn=SNS_TOPIC_ARN)
        #     return None, None
    except Exception as e:
        logger.error(f"Error In Payment DataFrame Processing:\n{e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # send_sns_notification(message="Error In Payment DataFrame Processing", subject='Response From DIL ETL Script', TopicArn=SNS_TOPIC_ARN)
        return None, None
    
def process_payment_df(payment_df,payment_df_rename_mapping):
    try:
        grouped_df_megre_with_header_df_columns = ['Header_Id','Payment_Mode', 'Gift_Card', 'Gift_Card_Amount']
        # payment_df = final_header_df.select(*payment_df_columns)
        # Flatten the PaymentsDetails
        logger.info(f"Started Flattening the Payment dataframe")
        final_payments_df = payment_df.withColumn('Payment', explode('PaymentsDetails.Payments')) \
                        .select(
                            'Header_Id',
                            'OrderId',
                            'TransactionNo',
                            'StoreCode',
                            'StoreName',
                            'BillClosetime',
                            'PosTerminalNo',
                            'Totalorderqty',
                            'Enriched_Order_Date',
                            'Enriched_Order_Time',
                            col("Payment.Amount").alias("Payment_Amount"),
                            col("Payment.Date").alias("Payment_Date"),
                            col("Payment.Mode").alias("Payment_Mode"),
                            col("Payment.TenderType").alias("Payment_TenderType")
                        )
        logger.info("AdDing Payment Df to cache")
        final_payments_df.cache()
        logger.info("Finished Flattening Payment Dataframe")
        logger.info("Started Grouping Payment dataframe to get gift cart and payment related data")
        grouped_df = final_payments_df.groupBy('Header_Id').agg(
                collect_list("Payment_Mode").alias("Payment_Mode"),
                collect_list("Payment_Amount").alias("PaymentAmounts_List"),
                array_contains(collect_list("Payment_Mode"), "Gift Card").alias("Gift_Card"),
                sum(when(col("Payment_Mode") == "Gift Card", col("Payment_Amount")).otherwise(0)).alias("Gift_Card_Amount")
            )
        #replace from [Multiple Payment mode], Square Brackets
        grouped_df = grouped_df.withColumn("Payment_Mode", regexp_replace(col("Payment_Mode").cast('string'), "[\[\]]", ""))
        gift_card_info_df = grouped_df.select(*grouped_df_megre_with_header_df_columns)
        logger.info("Finished Grouping Payment dataframe to get gift cart and payment related data")
        
        logger.info("Adding Payment id and Line No")
        # Add a new column with a sequential number partitioned by 'OrderId' and 'StoreCode'
        window_spec_Line_no = Window.partitionBy("OrderId", "StoreCode").orderBy("Enriched_Order_Date")
        final_payments_df = final_payments_df.withColumn("Line_No", F.row_number().over(window_spec_Line_no))
        final_payments_df = final_payments_df.withColumn("Payment_Id", concat_ws("_", "Header_Id", "Line_No"))
        final_payments_df = final_payments_df.withColumn("Payment_Date",date_format(to_date(col("Payment_Date"), "MM/dd/yy"), "yyyy/MM/dd"))
        final_payments_df = final_payments_df.withColumn("Payment_Amount",regexp_replace(col("Payment_Amount"), ",", "").cast("double"))
        logger.info("Added Payment id and Line No and renaming COlumn name for final payment dataframe")
        #Renaimg the Column Name as per the Final DIl Field
        final_payments_df = rename_payment_df(final_payments_df,payment_df_rename_mapping)
    
        return final_payments_df, gift_card_info_df
    except Exception as e:
        logger.error(traceback.format_exc())