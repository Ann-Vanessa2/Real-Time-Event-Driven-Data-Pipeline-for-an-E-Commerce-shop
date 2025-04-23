"""## Transformation Task"""

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

# DynamoDB table names
CATEGORY_TABLE = os.environ.get('CATEGORY_TABLE')
ORDER_TABLE = os.environ.get('ORDER_TABLE')

# S3_BUCKET_NAME = 'e-commerce-shop-a'
ARCHIVE_PREFIX = 'archive/'
RAW_PREFIX = 'raw-data/'
VALIDATED_PREFIX = 'validated/'
PROCESSED_PREFIX = 'processed/'

# s3 = boto3.client('s3')
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
ddb = boto3.resource('dynamodb', region_name=os.environ['AWS_REGION'])

DATE = datetime.today().date().isoformat()

# DynamoDB table names
# CATEGORY_TABLE = 'CategoryKPI'
# ORDER_TABLE = 'OrderKPI'

# Reading from validated folder
def read_csv_s3(key):
    """Read a CSV file from S3 and return its contents as a pandas DataFrame.

    Args:
        key (str): The S3 key of the CSV file to read.

    Returns:
        pandas.DataFrame: The contents of the CSV file as a DataFrame.
    """

    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    return pd.read_csv(response['Body'])

# products_df = read_csv_s3(VALIDATED_PREFIX + 'products.csv')
# orders_df = read_csv_s3(VALIDATED_PREFIX + 'orders.csv')
# order_items_df = read_csv_s3(VALIDATED_PREFIX + 'order_items.csv')

def run_transformation(products, orders, order_items):
    """
    Compute category-level and order-level KPIs from the given validated data.

    This function takes in the validated products, orders, and order items dataframes and
    returns two dataframes: one containing category-level KPIs and one containing order-level KPIs.

    The category-level KPIs include:

    - daily revenue
    - average order value
    - average return rate

    The order-level KPIs include:

    - total orders
    - total revenue
    - total items sold
    - return rate
    - unique customers

    Parameters:
        products (pandas.DataFrame): Validated products data.
        orders (pandas.DataFrame): Validated orders data.
        order_items (pandas.DataFrame): Validated order items data.

    Returns:
        tuple: A tuple of two pandas DataFrames: category-level KPIs and order-level KPIs.
    """

    # Create a map of product_id to category
    product_map = dict(zip(products['id'], products['category']))

    # Derive is_returned from returned_at
    orders['is_returned'] = orders['returned_at'].notna()

    # Merge order_items with orders
    merged = order_items.merge(orders, on='order_id', how='inner')

    # Add product category
    merged['category'] = merged['product_id'].map(product_map)

    # Create order_date column from created_at
    # merged['order_date'] = pd.to_datetime(merged['created_at']).dt.date

    # Compute total_price
    merged['total_price'] = merged['sale_price']  # Assuming this is per item and already accounts for quantity

    # Calculating Category-Level KPIs
    cat_kpi = (
        merged.groupby(['category', 'order_date'])
        .agg(
            daily_revenue=('total_price', 'sum'),
            order_count=('order_id', 'nunique'),
            return_count=('is_returned', 'sum')
        )
        .reset_index()
    )
    cat_kpi['avg_order_value'] = cat_kpi['daily_revenue'] / cat_kpi['order_count']
    cat_kpi['avg_return_rate'] = cat_kpi['return_count'] / cat_kpi['order_count']
    cat_kpi = cat_kpi.drop(columns=['order_count', 'return_count'])
    cat_kpi['daily_revenue'] = cat_kpi['daily_revenue'].round(2)
    cat_kpi['avg_order_value'] = cat_kpi['avg_order_value'].round(2)
    cat_kpi['avg_return_rate'] = cat_kpi['avg_return_rate'].round(4)
    cat_kpi['avg_return_rate'] = cat_kpi['avg_return_rate'] * 100  # Convert to percentage

    # Calculating Order-Level KPIs
    order_kpi = (
        merged.groupby('order_date')
        .agg(
            total_orders=('order_id', 'nunique'),
            total_revenue=('total_price', 'sum'),
            total_items_sold=('id', 'count'),  # 'id' from order_items (item-level ID)
            return_rate=('is_returned', 'mean'),  # Mean of True/False = percentage
            unique_customers=('user_id_x', 'nunique')  # '_x' might be needed if merged causes duplication
        )
        .reset_index()
    )
    order_kpi['total_revenue'] = order_kpi['total_revenue'].round(2)
    order_kpi['return_rate'] = order_kpi['return_rate'].round(4)
    order_kpi['return_rate'] = order_kpi['return_rate'] * 100  # Convert to percentage

    return cat_kpi, order_kpi

# Writing to DynamoDB
from decimal import Decimal

def write_to_dynamodb(cat_kpi, order_kpi):
    """
    Writes the category-level and order-level KPIs to DynamoDB.

    Args:
        cat_kpi (pd.DataFrame): The category-level KPIs.
        order_kpi (pd.DataFrame): The order-level KPIs.

    Returns:
        tuple: The category-level and order-level KPIs.
    """

    # Writing Category-Level KPIs to DynamoDB
    cat_table = ddb.Table(CATEGORY_TABLE)
    for _, row in cat_kpi.iterrows():
        # Convert 'order_date' to datetime if it's a string
        if isinstance(row['order_date'], str):
            row['order_date'] = pd.to_datetime(row['order_date']).date()

        item = {
            "category": row['category'],
            # "order_date": row['order_date'],
            "order_date": row['order_date'].isoformat(),  # Convert to ISO 8601 string
            # "daily_revenue": row['daily_revenue'],
            # "avg_order_value": row['avg_order_value'],
            # "avg_return_rate": row['avg_return_rate']
            "daily_revenue": Decimal(str(row['daily_revenue'])),
            "avg_order_value": Decimal(str(row['avg_order_value'])),
            "avg_return_rate": Decimal(str(round(row['avg_return_rate'], 2)))
        }
        cat_table.put_item(Item=item)
        # print(f"Saved Category KPI: {item}")
    print("Saved Category KPIs")

    # Writing Order-Level KPIs to DynamoDB
    order_table = ddb.Table(ORDER_TABLE)
    for _, row in order_kpi.iterrows():
        # Convert 'order_date' to datetime if it's a string
        if isinstance(row['order_date'], str):
            row['order_date'] = pd.to_datetime(row['order_date']).date()

        item = {
            "order_date": row['order_date'].isoformat(),  # Convert to ISO 8601 string
            "total_orders": row['total_orders'],
            "total_revenue": Decimal(str(row['total_revenue'])),
            "total_items_sold": row['total_items_sold'],
            "return_rate": Decimal(str(round(row['return_rate'], 2))),
            "unique_customers": row['unique_customers']
        }
        order_table.put_item(Item=item)
        # print(f"Saved Order KPI: {item}")
    print("Saved Order KPIs")

    return cat_kpi, order_kpi

# Write the data to s3 processed folder
def write_to_s3(cat_kpi, order_kpi):
    """
    Save category-level and order-level KPIs to S3 as CSV files with a timestamp.

    Args:
        cat_kpi (pd.DataFrame): The DataFrame containing category-level KPIs.
        order_kpi (pd.DataFrame): The DataFrame containing order-level KPIs.

    This function generates a timestamp to create unique S3 keys for the CSV files,
    converts the DataFrames to CSV format, and uploads them to the specified S3 bucket
    under the 'processed' prefix. It prints the S3 paths where the files are saved.
    """

    # Save according to date and time
    # timestamp = datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S") # %Y-%m-%d-T-%H:%M:%S
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-T-%H:%M:%S")

    # Convert DataFrame to CSV
    cat_csv_buffer = StringIO()
    cat_kpi.to_csv(cat_csv_buffer, index=False)

    order_csv_buffer = StringIO()
    order_kpi.to_csv(order_csv_buffer, index=False)

    # Define the S3 key for the CSV file with timestamp
    cat_key = f"{PROCESSED_PREFIX}{timestamp}/category_kpi.csv"
    order_key = f"{PROCESSED_PREFIX}{timestamp}/order_kpi.csv"

    # Upload the CSV file to S3
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=cat_key, Body=cat_csv_buffer.getvalue())
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=order_key, Body=order_csv_buffer.getvalue())

    print(f"Saved Category KPIs to: s3://{S3_BUCKET_NAME}/{cat_key}")
    print(f"Saved Order KPIs to: s3://{S3_BUCKET_NAME}/{order_key}")

    return cat_key, order_key

def archive_data(files):
    """
    Archive specified files from S3 to the archive directory with a timestamp.

    This function takes a list of S3 file keys, copies each file to an archive 
    directory within the same S3 bucket, appending a timestamp to the archive 
    path, and then deletes the original file from its location.

    Args:
        files (list): List of S3 keys representing the files to be archived.
    """

    timestamp = datetime.utcnow().strftime('%Y-%m-%d-T-%H:%M:%S')
    for key in files:
        # Copy to archive
        archive_key = ARCHIVE_PREFIX + timestamp + '/' + key.split('/', 1)[1]
        s3.copy_object(Bucket=S3_BUCKET_NAME, CopySource={'Bucket': S3_BUCKET_NAME, 'Key': key}, Key=archive_key)

        # Delete original
        s3.delete_object(Bucket=S3_BUCKET_NAME, Key=key)

        print(f"\nArchived data to: s3://{S3_BUCKET_NAME}/{archive_key}")

    # Archive processed files
all_files = [RAW_PREFIX + 'products.csv']
all_files += [obj['Key'] for obj in s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=RAW_PREFIX + 'orders/').get('Contents', []) if obj['Key'].endswith('.csv')]
all_files += [obj['Key'] for obj in s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=RAW_PREFIX + 'order_items/').get('Contents', []) if obj['Key'].endswith('.csv')]


def main():
    try:
        print("Starting ECS Task: Transformation Job")

        print("Loading Validated Data...")
        products_df = read_csv_s3(VALIDATED_PREFIX + 'products.csv')
        orders_df = read_csv_s3(VALIDATED_PREFIX + 'orders.csv')
        order_items_df = read_csv_s3(VALIDATED_PREFIX + 'order_items.csv')

        print("Running Transformation...")
        merged = run_transformation(products_df, orders_df, order_items_df)

        # write_to_dynamodb(cat_kpi, order_kpi)
        # write_to_s3(cat_kpi, order_kpi)

        print("Writing to DynamoDB...")
        write_to_dynamodb(merged[0], merged[1])
        print("Successfully written to DynamoDB.")

        print("Writing to S3...")
        write_to_s3(merged[0], merged[1])
        print("Successfully written to S3.")

        print("Archiving Data...")
        archive_data(all_files)

    except Exception as e:
        print("Error during processing:", e)
        sys.exit(1)

    print("Transformation Completed Successfully.")

if __name__ == "__main__":
    main()