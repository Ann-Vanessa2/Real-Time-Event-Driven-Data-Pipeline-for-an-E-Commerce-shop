"""## Validation Task"""

import boto3, os
import pandas as pd
from datetime import datetime
from io import StringIO
import sys
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# Set up AWS creds
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.environ.get('AWS_REGION', 'eu-west-1')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

# S3_BUCKET_NAME = 'e-commerce-shop-a'
ARCHIVE_PREFIX = 'archive/'
RAW_PREFIX = 'raw-data/'
VALIDATED_PREFIX = 'validated/'

# s3 = boto3.client('s3')
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

DATE = datetime.today().date().isoformat()

product_file = f"{RAW_PREFIX}products.csv"
orders_prefix = f"{RAW_PREFIX}orders/"
order_items_prefix = f"{RAW_PREFIX}order_items/"


def s3_files_exist(prefix):
  """Check if any files exist at the given S3 prefix.

  Args:
    prefix (str): The S3 prefix to check.

  Returns:
    bool: True if any files exist at the given prefix, False otherwise.
  """
  prefix = prefix.replace('//', '/')
  response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
  return 'Contents' in response and len(response['Contents']) > 0

def check_required_files():
    """Check that all required files exist in the S3 bucket.

    Raises a FileNotFoundError if any required files are missing.
    """
    missing = []
    if not s3_files_exist(product_file):
        missing.append("products.csv")
    if not s3_files_exist(orders_prefix):
        missing.append("orders/")
    if not s3_files_exist(order_items_prefix):
        missing.append("order_items/")
    
    if missing:
        raise FileNotFoundError(f"Required files not found: {', '.join(missing)}")
    # return missing


# if not s3_files_exist(product_file):
#     raise FileNotFoundError("products.csv not found!")

# if not s3_files_exist(orders_prefix):
#     raise FileNotFoundError("No files found in orders/ folder!")

# if not s3_files_exist(order_items_prefix):
#     raise FileNotFoundError("No files found in order_items/ folder!")

# print("All input files exist.")

def read_csv_s3(key):
    """Read a CSV file from S3 and return a pandas DataFrame.

    Args:
        key (str): The key of the CSV file to read.

    Returns:
        pandas.DataFrame: The contents of the CSV file.
    """
    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    return pd.read_csv(response['Body'])

def read_all_csvs(prefix):
    """Read all CSV files from S3 at the given prefix and return a pandas DataFrame
    concatenated from all the files.

    Args:
        prefix (str): The S3 prefix at which to read the CSV files.

    Returns:
        pandas.DataFrame: The concatenated contents of all the CSV files.
    """
    resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    dfs = []
    for obj in resp.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            df = read_csv_s3(obj['Key'])
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def run_validation():
    """
    Validates and cleans the product, order, and order item data.

    This function reads product, order, and order item data from S3, 
    performs data cleaning by dropping null and invalid entries, 
    ensures referential integrity by filtering order items with valid orders, 
    and extracts date information from timestamp fields.

    Returns:
        tuple: A tuple containing three pandas DataFrames for products, orders, and order items,
        which have been validated and are ready for further processing.
    """

    products = read_csv_s3(product_file)
    orders = read_all_csvs(orders_prefix)
    order_items = read_all_csvs(order_items_prefix)

    # Drop nulls and invalids
    orders = orders.dropna(subset=['order_id', 'user_id', 'created_at'])
    order_items = order_items.dropna(subset=['id', 'product_id', 'sale_price'])
    order_items = order_items[order_items['sale_price'] > 0]

    # Filter by referential integrity
    valid_order_ids = set(orders['order_id'])
    order_items = order_items[order_items['order_id'].isin(valid_order_ids)]

    # Extracting the date from created_at and returned_at
    # Create order_date column from created_at
    orders['order_date'] = pd.to_datetime(orders['created_at']).dt.date
    orders['return_date'] = pd.to_datetime(orders['returned_at']).dt.date

    # # Creating an is-retunred column
    # orders['is_returned'] = orders['returned_at'].notna()

    return products, orders, order_items

# products_df, orders_df, order_items_df = run_validation()
# print("Validation complete.")


# Saving the data to s3 "validated" folder
def save_to_s3(df, name):
    """
    Save a pandas DataFrame to S3 as a CSV file.

    Args:
        df (pandas.DataFrame): The DataFrame to save.
        name (str): The name of the file to save the data to, without the .csv extension.
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=VALIDATED_PREFIX + name, Body=csv_buffer.getvalue())

print("Validation data saved to S3.")

def main():
    """
    Main entry point for the script.

    This function validates and cleans the product, order, and order item data, and saves it to S3.
    If any required files are missing, it raises a FileNotFoundError.
    If any other exception occurs during execution, it prints an error message and exits with a non-zero status code.

    Returns:
        int: The exit status of the script.
    """
    try:
        print("Checking for required files...")
        check_required_files()
        print("All input files exist.")

        print("Validating data...")
        products_df, orders_df, order_items_df = run_validation()

        save_to_s3(products_df, "products.csv")
        save_to_s3(orders_df, "orders.csv")
        save_to_s3(order_items_df, "order_items.csv")

        print("Validation complete and saved to S3.")

        sys.exit(0)

    except Exception as e:
        print("Error during processing:", e)
        sys.exit(1)

    print("Validation complete. Data is ready for transformation.")

if __name__ == "__main__":
    main()