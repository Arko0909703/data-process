from config import *
from custom_loggers import *

def get_baseline_df(final_header_df, mapping_dict, final_columns):
    print(f"Available Columns: {final_header_df.columns}")
    # Rename columns based on the provided mapping
    for old_name, new_name in mapping_dict.items():
        if old_name in final_header_df.columns:
            # logger.info(f"Remnaming Column from {old_name} to {new_name}")
            final_header_df = final_header_df.withColumnRenamed(old_name, new_name)
    logger.info(f"Columns to Select for Baseline_df: {final_columns}")
    baseline_df = final_header_df.select(*final_columns)
    return baseline_df

def get_mapping_df(final_header_df, mapping_dict, final_columns):
    # Rename columns based on the provided mapping
    for old_name, new_name in mapping_dict.items():
        if old_name in final_header_df.columns:
            # logger.info(f"Remnaming Column from {old_name} to {new_name}")
            final_header_df = final_header_df.withColumnRenamed(old_name, new_name)
    logger.info(f"Columns to Select for Mapping_df: {final_columns}")
    mapping_df = final_header_df.select(*final_columns)
    return mapping_df

def get_pii_df(final_header_df, mapping_dict, final_columns):
    # Rename columns based on the provided mapping
    for old_name, new_name in mapping_dict.items():
        if old_name in final_header_df.columns:
            # logger.info(f"Remnaming Column from {old_name} to {new_name}")
            final_header_df = final_header_df.withColumnRenamed(old_name, new_name)
    pii_df = final_header_df.select(*final_columns)
    return pii_df