from config import *
import psutil
from datetime import datetime
def log_dataframe_size(df, name):
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    size_in_mb = size_in_bytes / (1024 * 1024)
    size_in_gb = size_in_bytes / (1024 * 1024 * 1024)
    
    # Log the size in MB and GB
    print(f"Size in MB {name}: {size_in_mb:.2f} MB")
    print(f"Size in GB {name}: {size_in_gb:.2f} GB")
    # logger.info(df.explain(mode='cost'))

    
def log_available_memory():
    memory_info = psutil.virtual_memory()
    # Available memory in bytes
    available_memory = memory_info.available
    
    # Convert to MB for readability
    available_memory_mb = available_memory / (1024 * 1024)

    logger.info(f"Available Memory: {available_memory_mb:.2f} MB")



def get_existing_dataframe(path,spark):
    try:
        df = spark.read.format('parquet') \
            .option('header', True) \
            .option('delimiter', ',')\
            .option("encoding", "UTF-8")\
            .load(f'{path}')
        return df
    except AnalysisException as e:
        print(f"The file at '{path}' was not found, So Saving Whole Data")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def save_dataframe(df, path, type, date_col, spark):
    
    unique_dates = df.select(date_col).distinct().collect()
    print(unique_dates)
    print("+++"*35)
    print(unique_dates)
    print("+++"*35)
    for row in unique_dates:
        date_path = row[date_col]
        date_obj = datetime.strptime(row[date_col], '%Y-%m-%d')
        date_path_s3 = date_obj.strftime('%Y/%m/%d')
        # Construct S3 path based on date
        # output_path = f"{path.rstrip('/')}/{type}/{date_path}/"
        output_path = f"{path.rstrip('/')}/{type}/{date_path_s3}/"
        print(f"Checking for existing data on Date: {date_path} at {output_path}")
        try:
            # Get existing Header_Ids for the specific date path and convert to a list
            existing_df = get_existing_dataframe(output_path, spark)
            # print(f"Existing Header_Ids for Date {date_path}: {existing_df.count()}")
            # print("***"*20)
            # print(f"Existing DataFrame Columns: {existing_df.columns}")
        except Exception as e:
            print(f"No Existing Dataframe Found for :-{date_path}")
            
        # Filter the main DataFrame by date and check if existing records are present
        df_date = df.filter(F.col(date_col) == date_path)
        total_count = df_date.count()  # Total records for the date
        # print(f"Total Data Count:{total_count}")
        if existing_df and not existing_df.isEmpty():
            # print(f"Data Frame Columns: {df_date.columns}")
            # print(f"***"*20)
            # print(f"Existing Columns: {df_date.columns}")

            # Subtract records in existing_df from df_date based on Header_Id
            # df_filtered = df_date.subtract(existing_df)
            df_filtered = df_date.join(existing_df, on="Header_Id", how="left_anti")
            save_count = df_filtered.count()
            # print(f"save_count:{save_count}")
        else:
            # No existing data for the date, so save all records for the date
            df_filtered = df_date
            save_count = total_count
            # print(f"Data Saved Count:{save_count}")
        # Calculate skipped records
        skipped_count = total_count - save_count
        # print(f"Alredy Available Data:{skipped_count}")
        if save_count > 0:
            try:
                # Save DataFrame to S3
                log_available_memory()
                log_dataframe_size(df_filtered,"df_filtered")
                df_filtered.write.option('header', True).mode("append").parquet(output_path)
                
                df = df.filter(F.col(date_col) != date_path)
                print(f"Data saved for Date: {date_path} at {output_path}")
                print(f"Total records: {total_count}, Saved: {save_count}, Skipped: {skipped_count}")
            except Exception as e:
                print(f"Failed to save data for Date: {date_path} at {output_path}")
                print(f"Error: {str(e)}")
            finally:
                del df_filtered
                  
        else:
            print(f"No new data to save for Date: {date_path}")
            print(f"Total records: {total_count}, Saved: {save_count}, Skipped: {skipped_count}")
        print("***"*30)
        