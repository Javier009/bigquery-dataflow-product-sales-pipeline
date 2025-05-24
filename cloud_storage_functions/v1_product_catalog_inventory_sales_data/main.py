import math
import random
import pytz
from io import StringIO
import csv
from datetime import datetime, timedelta
from faker import Faker
from flask import Request

# Supporting Functions imports
from utils.supporting_functions import *

# Import Cloud Storage 
from google.cloud import storage

fake = Faker()

# Google Cloud project
PROJECT_ID  = "bigquery-dataflow-460522"

# Google Cloud Storage Bucket 
STORAGE_BUCKET = 'retail_data_v1'
storage_client = storage.Client()
bucket_instance = storage_client.bucket(STORAGE_BUCKET)

# --- Dates Configuration ---
pacific_tz = pytz.timezone('America/Los_Angeles') 
utc_now = datetime.now(pytz.utc)
pacific_time_now = utc_now.astimezone(pacific_tz)
today_date_object = pacific_time_now - timedelta(hours=1) # To get complete data from prev hour
TODAY = today_date_object.strftime('%Y-%m-%d')
TODAY_LATEST_FULL_HOUR = today_date_object.strftime('%H')


start_date_obj = today_date_object - timedelta(days=2)  # Change for backfill or run check on more dates
START_DATE = start_date_obj.strftime('%Y-%m-%d')
START_DATE_HOUR = '00'

NUM_PRODUCTS = random.randint(50,100)
STATIC_PRODUCT_ARCHETYPES = generate_static_products()

# product_catalog = create_static_product_catalog(NUM_PRODUCTS,STATIC_PRODUCT_ARCHETYPES,'2025-01-01', '13')
# pre_sales_inventory = generate_random_pre_sales_inventory(product_catalog, '2025-01-01', '13')
# product_sales = generate_sales_data(product_catalog, '2025-01-01', '13')
# product_inventory = update_inventory(product_sales,pre_sales_inventory) 

dates = generate_date_list(START_DATE,TODAY)
hours = [ f"{h:02d}" for h in range(24)]

def generate_data(request:Request):
    try:
        for date in dates:
            for hour in hours:
                if date == TODAY and hour > TODAY_LATEST_FULL_HOUR:
                    pass
                else:
                    catalog_blob_name = f'product_catalog/date={date}/hour={hour}/product_catalog_for_{date}-{hour}.csv'
                    sales_blob_name = f'sales_data/date={date}/hour={hour}/sales_data_for_{date}-{hour}.csv'
                    inventory_blob_name = f'inventory_data/date={date}/hour={hour}/sales_data_for_{date}-{hour}.csv'

                    catalog_blob = bucket_instance.blob(catalog_blob_name)
                    sales_blob = bucket_instance.blob(sales_blob_name)
                    inventory_blob = bucket_instance.blob(inventory_blob_name)

                    # Product Catalog
                    if catalog_blob.exists():
                        print(f'{catalog_blob_name}  exist, no taking actions')
                    else:
                        print(f'{catalog_blob_name} does not exists, creating file')
                        product_catalog = create_static_product_catalog(NUM_PRODUCTS,STATIC_PRODUCT_ARCHETYPES,date, hour)
                        pre_sales_inventory = generate_random_pre_sales_inventory(product_catalog, date, hour)  # Fake an initial inventory                    
                        upload_tuples_to_gcs_as_csv(STORAGE_BUCKET, bucket_instance, catalog_blob_name, product_catalog) # Upload Product Catalog

                    # Sales data
                    if sales_blob.exists():
                        print(f'{sales_blob_name} exist, no taking actions') 
                    else:
                        print(f'{sales_blob_name} does not exists, creating file')
                        product_sales = generate_sales_data(product_catalog, date, hour)                    
                        upload_tuples_to_gcs_as_csv(STORAGE_BUCKET, bucket_instance, sales_blob_name, product_sales) # Upload Sales Catalog

                    # Inventory Data
                    if inventory_blob.exists():
                        print(f'{inventory_blob_name} exist, no taking actions') 
                    else:
                        print(f'{inventory_blob_name} does not exists, creating file')
                        product_inventory = update_inventory(product_sales,pre_sales_inventory) 
                        upload_tuples_to_gcs_as_csv(STORAGE_BUCKET, bucket_instance, inventory_blob_name, product_inventory) # Upload Inventory Catalog
        return 'Data generated and uploaded succesfully', 200
    except Exception as e:
        return  f"❌ Errors encountered plase review, {e} rows", 500