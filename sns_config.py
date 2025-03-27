import logging
import boto3
from typing import Optional
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
)

# Define the default SNS topic ARN
# # TOPIC_ARN = 'arn:aws:sns:ap-south-1:569056949002:Glue_ETL_Topic'
# TOPIC_ARN = 'arn:aws:sns:ap-south-1:569056949002:Glue_ETLs_Topic-New'
# SNS topic ARN
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:569056949002:Response'
MISSING_COMBINATION_ARN = 'arn:aws:sns:ap-south-1:569056949002:Dil_missing_combination_id'

# def get_verbose_timestamp() -> str:
#     """
#     Get the current timestamp in a verbose format.
    
#     Returns:
#         str: The current timestamp in 'Day, Month Date, Year Hour:Minute:Second AM/PM' format.
#     """
#     now = datetime.now()
#     return now.strftime('%A, %B %d, %Y %I:%M:%S %p')

def send_sns_notification(
    message: str,
    subject: str,
    job_name: str,
    etl_start_time: float,
    topic_arn: Optional[str] = None,
    sns_client: Optional[boto3.client] = None
) -> None:
    """
    Send an SNS notification with the specified message and subject.

    Args:
        message (str): The message body of the notification.
        subject (str): The subject line of the notification.
        job_name (str): The name of the job or process.
        topic_arn (Optional[str]): The ARN of the SNS topic. If not provided, will use a default value.
        sns_client (Optional[boto3.client]): The SNS client to use for sending the notification. If None, a default client is used.
    """
    if sns_client is None:
        sns_client = boto3.client('sns')

    if topic_arn is None:
        topic_arn = TOPIC_ARN

    elapsed_time = time.time() - etl_start_time  # Calculate elapsed time    
    elapsed_minutes = elapsed_time / 60
    # timestamp = get_verbose_timestamp()

    # Format the message
    # formatted_message = (
    #     f"Timestamp: {timestamp}\n"
    #     f"Job Name: {job_name}\n\n"
    #     f"Message:\n{message}"
    # )

    formatted_message = (
        f"Job Name: {job_name}\n\n"
        f"Elapsed Time: {elapsed_minutes:.2f} minutes\n\n"
        f"Message:\n{message}"
    )

    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=formatted_message,
            Subject=subject
        )
        logging.info(f"SNS notification sent with subject: {subject}")
    except sns_client.exceptions.ClientError as e:
        logging.error(f"Failed to send SNS notification: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


