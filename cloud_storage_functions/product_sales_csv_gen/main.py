# Regular imports 
import random
import datetime
import io
import csv
from faker import Faker
from flask import Request

# Import Cloud Storage 
from google.cloud import storage

PROJECT_ID  = "bigquery-dataflow-460522"

# Google Cloud Storage Bucket
STORAGE_BUCKET = 'retail_data_v1'
storage_client = storage.Client()
bucket_instance = storage_client.bucket(STORAGE_BUCKET)

# Faker for dat generation
fake = Faker()

# --- Configuration ---
NUM_PRODUCTS = random.randint(50,100)
NUM_CATEGORIES = random.randint(5,10)
TODAY = datetime.date.today().strftime('%Y-%m-%d')

# --- Helper Functions ---
def generate_categories(num_categories):
    categories = []
    for i in range(1, num_categories + 1):
        category_id = f"CAT_{i:03d}"
        category_name = fake.word().capitalize() + " Goods" # e.g., "Digital Goods", "Garden Goods"
        categories.append({"id": category_id, "name": category_name})
    return categories

import datetime

def generate_date_list(start_date_str, end_date_str):
    date_list = []
    try:
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("Error: Ensure dates are in 'YYYY-MM-DD' format.")
        return date_list

    if start_date > end_date:
        print("Error: Start date cannot be after end date.")
        return date_list

    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=1)
    
    return date_list


def generate_product_catalog_data(num_products, ingestion_date):
    products_data = []
    headers = ('product_id','product_name','category_id','category_name','brand','description','unit_price','supplier_name','tags', 'ingestion_date')
    products_data.append(headers)
    categories = generate_categories(NUM_CATEGORIES)
    product_adjectives = ["Premium", "Value", "Eco-Friendly", "Smart", "Heavy-Duty", "Compact", "Designer"]
    product_nouns = ["Widget", "Gadget", "Device", "Appliance", "Tool", "Kit", "System"]

    for i in range(1, num_products + 1):
        product_id = f"PROD_{i:04d}"
        chosen_category = random.choice(categories)
        category_id = chosen_category["id"]
        category_name = chosen_category["name"]

        product_name = f"{random.choice(product_adjectives)} {fake.word().capitalize()} {random.choice(product_nouns)}"
        brand = fake.company()
        description = fake.sentence(nb_words=10)
        unit_price = round(random.uniform(5.99, 799.99), 2)
        supplier_name = fake.company_suffix() + " " + fake.last_name() + " Supplies" 
        tags = random.sample(["new_arrival", "best_seller", "clearance", "eco_friendly", "premium_quality", "limited_edition"], k=random.randint(1, 3))

        products_data.append((
            product_id,
            product_name,
            category_id,
            category_name,
            brand,
            description,
            unit_price,
            supplier_name,
            ",".join(tags), 
            ingestion_date
        ))
    return products_data

def generate_sales_data(product_catalog:list, sales_date):
    sales_data = []
    headers = ('transaction_date', 'transaction_id', 'customer_id', 'order_country', 'product_id', 'unit_price', 'units_sold', 'discount_applied', 'total_ammount_paid')
    sales_data.append(headers)
    num_sales = random.randint(150,1000) # A random number of sales
    product_catalog_rows = product_catalog[1:]
    for i in range(num_sales):
        transaction_date = sales_date
        transaction_id = fake.uuid4()
        customer_id = f"CUST_{random.randint(1001, 5000):04d}"
        order_country = random.choice(['United States', 'Canada', 'Mexico', 'Brazil', 'Argentina', 'UK', 'France', 'Germany', 'China', 'Spain'])
        product_sold_index = random.randint(0,len(product_catalog_rows))  # Tupple
        product_id = product_catalog_rows[product_sold_index-1][0]
        unit_price = product_catalog_rows[product_sold_index-1][6]
        units_sold = random.randint(1, 10)

        if units_sold > 5:
            discount_applied = random.randint(5, 20) / 100
        else:
            discount_applied = 0 

        total_ammount_paid = (unit_price *(1-discount_applied)) * units_sold

        sales_data.append((transaction_date,
                        transaction_id,
                        customer_id,
                        order_country,
                        product_id,
                        unit_price,
                        units_sold, 
                        discount_applied,
                        total_ammount_paid
                        ))
        
    return sales_data

def upload_tuples_to_gcs_as_csv(
    destination_blob_name: str,
    data_tuples_with_headers: list # Renamed for clarity
):
    try:
        bucket = bucket_instance # Using global instance
        blob = bucket.blob(destination_blob_name)

        csv_string_buffer = io.StringIO()
        csv_writer = csv.writer(csv_string_buffer)

        # data_tuples_with_headers already contains the header row as its first element
        csv_writer.writerows(data_tuples_with_headers) 

        csv_content = csv_string_buffer.getvalue()
        csv_string_buffer.close()

        blob.upload_from_string(csv_content, content_type='text/csv')
        print(f"Successfully uploaded data to gs://{STORAGE_BUCKET}/{destination_blob_name}")

    except Exception as e:
        print(f"An error occurred while uploading {destination_blob_name}: {e}")
        
# Send data to Cloud storage


def genearete_product_and_sales_data(request: Request):
    
    try:
        dates = generate_date_list('2025-01-01',TODAY)
        for date in dates:

            catalog_blob_name = f'product_catalog/product_catalog_for_{date}.csv'
            sales_blob_name = f'sales_data/sales_data_for_{date}.csv'

            catalog_blob = bucket_instance.blob(catalog_blob_name)
            sales_blob = bucket_instance.blob(sales_blob_name)

            if catalog_blob.exists() and sales_blob.exists():
                print(f'{catalog_blob_name} and {sales_blob_name}  exist, no taking actions')
            else:
                print(f'{catalog_blob_name} and {sales_blob_name} dont not exists, creating files')

                products_catalog = generate_product_catalog_data(NUM_PRODUCTS, date)
                sales_data = generate_sales_data(products_catalog, date)

                #Upload to GCS
                # Product Catalog
                upload_tuples_to_gcs_as_csv(catalog_blob_name, products_catalog)
                # Sales data
                upload_tuples_to_gcs_as_csv(sales_blob_name, sales_data)

        return 'Succesfully read GCS files and uploaded pending files if any ', 200
    except Exception as e:
        return  f"❌ Errors encountered plase review, {e} rows", 500
        
            
