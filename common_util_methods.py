from pii_config import *
from config import *

# # Load key and IV from S3
# def load_key():
#     try:
#         response = s3_client.get_object(Bucket='ph-etl-archive', Key='Data Normalisation DIL/key_config.json')
#         config_data = json.loads(response['Body'].read().decode('utf-8'))
#         logger.info("Key and IV loaded from AWS S3 bucket")
#         return config_data["secret_key"], config_data["iv"]
#     except Exception as e:
#         error_message: str = f"Error occurred while loading key and iv from AWS S3: {e}"
#         logger.error(error_message)
#         logger.error(f"Traceback: {traceback.format_exc()}")
#         # send_sns_notification(error_message, "Key and IV Loading Error")
#         raise


# get secrets from AWS Secrets Manager
def get_secret(secret_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')
 
    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)
 
        # Check if the secret is stored as a string
        if 'SecretString' in response:
            secret = response['SecretString']
        else:
            raise ValueError("Secret is not stored as a string")
 
        return json.loads(secret)
 
    except ClientError as e:
        # Handle specific AWS service errors
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logger.error("Secrets Manager can't decrypt the protected secret text using the provided KMS key.")
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logger.error("An error occurred on the server side.")
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("You provided an invalid value for a parameter.")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("You provided a parameter value that is not valid for the current state of the resource.")
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("The requested secret was not found.")
        else:
            logger.error(f"An unexpected error occurred: {e.response['Error']['Message']}")
        raise e
 
    except NoCredentialsError:
        logger.error("Credentials not available.")
        raise
 
    except EndpointConnectionError:
        logger.error("Could not connect to the endpoint URL.")
        raise
 
    except ValueError as ve:
        logger.error(f"Error processing secret value: {ve}")
        raise
 
    except Exception as e:
        # General exception handler for any other errors
        logger.error(f"An error occurred: {e}")
        raise

# Load key and IV from S3
def load_key():
    try:
        # response = s3_client.get_object(Bucket='ph-etl-archive', Key='Data Normalisation DIL/key_config.json')
        # config_data = json.loads(response['Body'].read().decode('utf-8'))
        secret_name = "Data_Normalization_AES256_Secrets"
        secret_data = get_secret(secret_name)
        if secret_data: 
            logger.info("Key and IV extracted from AWS Secrets Manager")
            return secret_data["secret_key"], secret_data["iv"]
        else:
            raise ValueError("Secrets Not found!")

    except Exception as e:
        error_message: str = f"Error occurred while loading key and iv : {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        # send_sns_notification(error_message, "Key and IV Loading Error")
        raise


# Encryption and Decryption functions
def encrypt_mobile(text, secret_key,iv) :
    try:
        if text is None or len(text)==0:
            return ""
        cipher = AES.new(secret_key.encode(), AES.MODE_CBC, iv.encode())
        encrypted_bytes = cipher.encrypt(pad(text.encode(), AES.block_size))
        return base64.b64encode(encrypted_bytes).decode()
    except Exception as e:
        error_message = f"Error in encrypting {text}: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        # send_sns_notification(error_message, "Encryption Error")
        return None

def decrypt_mobile(encrypted_text,secret_key,iv):
    try:
        cipher = AES.new(secret_key.encode(), AES.MODE_CBC, iv.encode())
        decrypted_bytes = unpad(cipher.decrypt(base64.b64decode(encrypted_text)), AES.block_size)
        return decrypted_bytes.decode()
    except Exception as e:
        error_message = f"Error in decrypting {encrypted_text}: {e}"
        logger.error(error_message)
        logger.error(f"Traceback: {traceback.format_exc()}")
        # send_sns_notification(error_message, "Decryption Error")
        return None

# Batch write function for DynamoDB
def batch_write(items, table_name):
    try:
        with table_name.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
    except Exception as e:
        logger.error(f"Error during batch_write: {e}")
        logger.error(f"Failed batch:{items}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # send_sns_notification(f"Error during batch_write: {e}", "Batch Write Error")