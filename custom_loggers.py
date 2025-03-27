from config import *
def setup_in_memory_logging():
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger, log_stream

def Save_log_file(logger, log_stream, s3_client, bucket = 'ph-etl-archive', log_output_folder='Data Normalisation DIL/MultiThreading Application/glue-job-logs',log_file_name='log'):
    logger.info(f"log_output_folder: {log_output_folder}")
    log_file_path = f'{log_output_folder}/{log_file_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'
    logger.info(f"log_file_path: {log_file_path}")
    # Flush the logs
    for handler in logger.handlers:
        handler.flush()
    log_content = log_stream.getvalue()
    s3_client.put_object(Bucket=bucket, Key =log_file_path, Body=log_content)
    return f"s3://{bucket}/{log_file_path}"
    
logger, log_stream = setup_in_memory_logging()