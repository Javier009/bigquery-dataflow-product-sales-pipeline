import math
import random
from io import StringIO
import io
import csv
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def generate_static_products():
    STATIC_PRODUCT_ARCHETYPES = [
    {
        "base_name": "UltraSmart",
        'category_id' : 'CAT-01',
        "category": "Electronics",
        "brand": "TechNova",
        "description_suffix": "Cutting-edge features for modern life.",
        "unit_price": 10.43,
        "supplier": "Global Gadget Co.",
        "tags": "smart, connected, high-tech, innovation"
    },
    {
        "base_name": "Eco-Comfort",
        'category_id' : 'CAT-02',
        "category": "Home Goods",
        "brand": "GreenLiving",
        "description_suffix": "Sustainable and cozy additions to your home.",
        "unit_price": 20.987,
        "supplier": "EcoHome Supplies",
        "tags": "eco-friendly, sustainable, comfort, organic"
    },
    {
        "base_name": "AdventurePro",
        'category_id' : 'CAT-03',
        "category": "Sports & Outdoors",
        "brand": "SummitGear",
        "description_suffix": "Durable gear for all your outdoor excursions.",
        "unit_price": 25.23,
        "supplier": "Outdoor Ventures Inc.",
        "tags": "durable, outdoor, adventure, performance"
    },
    {
        "base_name": "GourmetBlend",
        'category_id' : 'CAT-04',
        "category": "Food & Beverages",
        "brand": "ArtisanEats",
        "description_suffix": "Finest ingredients for exquisite culinary creations.",
        "unit_price": 12.23,
        "supplier": "Culinary Delights Ltd.",
        "tags": "gourmet, organic, fresh, artisanal"
    },
    {
        "base_name": "SoftWear",
        'category_id' : 'CAT-05',
        "category": "Apparel",
        "brand": "ComfortFit",
        "description_suffix": "Designed for maximum comfort and style.",
        "unit_price": 40.43,
        "supplier": "Textile Innovations",
        "tags": "comfortable, stylish, breathable, quality"
    },
    { 
        "base_name": "ProCraft",
        'category_id' : 'CAT-06',
        "category": "Tools & Hardware",
        "brand": "BuildStrong",
        "description_suffix": "Precision tools for every project, big or small.",
        "unit_price": 34.87,
        "supplier": "Industrial Solutions Co.",
        "tags": "durable, heavy-duty, professional, precision"
    },
    { 
        "base_name": "PetLove",
        'category_id' : 'CAT-07',
        "category": "Pet Supplies",
        "brand": "FurryFriends",
        "description_suffix": "Premium products to keep your beloved pets happy and healthy.",
        "unit_price": 23.65,
        "supplier": "Animal Care Distributors",
        "tags": "natural, pet-safe, nutritious, durable"
    },
    { 
        "base_name": "BrightFuture",
        'category_id': 'CAT-08',
        "category": "Education",
        "brand": "LearnSmart",
        "description_suffix": "Innovative learning resources for all ages.",
        "unit_price": 67.34,
        "supplier": "EdTech Global",
        "tags": "interactive, educational, engaging, digital"
    },
    {
        "base_name": "VitalityBoost",
        'category_id' : 'CAT-09',
        "category": "Health & Wellness",
        "brand": "PureLife",
        "description_suffix": "Supplements and aids for a balanced and healthy lifestyle.",
        "unit_price": 78.4,
        "supplier": "Natural Health Co.",
        "tags": "organic, natural, supplement, energy"
    }
    ]   

    return STATIC_PRODUCT_ARCHETYPES

def generate_date_list(start_date_str, end_date_str):
    date_list = []
    try:
        # datetime.strptime returns a datetime object, then .date() extracts only the date part
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("Error: Ensure dates are in 'YYYY-MM-DD' format.")
        return date_list

    if start_date > end_date:
        print("Error: Start date cannot be after end date.")
        return date_list

    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        # Fix: Use 'timedelta' directly, not 'datetime.timedelta'
        current_date += timedelta(days=1) # <-- Corrected line

    return date_list


def create_static_product_catalog(num_products, prod_archetypes, ingestion_date, ingestion_hour):

    all_products = []
    headers = ['ingestion_date', 'ingestion_hour','product_id', 'product_name', 'category_id,','category_name', 'brand', 'description', 'unit_price', 'supplier_name', 'tags']
    all_products.append(headers)
    num_archetypes = len(prod_archetypes)

    for i in range(num_products):
        product_id_num = i + 1
        product_id = f"PROD_{product_id_num:04d}" # Formats as PROD_0001, PROD_0002, etc.

        # Select archetype based on cycle to ensure static assignment
        archetype_index = i % num_archetypes
        archetype = prod_archetypes[archetype_index]

        # Generate unique product name based on archetype and product number
        # This makes each product distinct while maintaining category consistency
        product_name = f"{archetype['base_name']} {product_id_num:02d} - {math.ceil((i + 1) / num_archetypes)}"

        product_details = (
            ingestion_date,
            ingestion_hour,
            product_id,
            product_name,
            archetype["category_id"],
            archetype["category"],
            archetype["brand"],
            f"A high-quality product. {archetype['description_suffix']}",
            archetype["unit_price"],            
            archetype["supplier"],
            archetype["tags"],
        )
        all_products.append(product_details)

    return all_products

# Generate the static product dictionary


def generate_random_pre_sales_inventory(product_catalog:list, inventory_date, inventory_hour):
    inventory = {}
    for prod_tup in product_catalog[1:]:
        product_id = prod_tup[2]
        in_stock = random.randint(2000,7000)
        new_product = random.randint(2000,7000)
        returned_product = random.randint(-2000,0)
        
        product_inventory_detail = {'inventory_date': inventory_date,
                                    'inventory_hour': inventory_hour,
                                    'in_stock' : in_stock,
                                    'new_product' : new_product,
                                    'returned_product': returned_product,                                 
                                    'units_sold' : 0
                                    }
        
        inventory[product_id] = product_inventory_detail

    return inventory



def generate_sales_data(product_catalog:list, sales_date, sales_hour):
    sales_data = []
    headers = ('transaction_date', 'transaction_hour', 'transaction_id', 'customer_id', 'order_country', 'product_id', 'unit_price', 'units_sold', 'discount_applied', 'total_ammount_paid')
    sales_data.append(headers)
    num_sales = random.randint(150,1000) # A random number of sales
    product_catalog_rows = product_catalog[1:]
    for i in range(num_sales):
        transaction_date = sales_date
        transaction_hour = sales_hour
        transaction_id = fake.uuid4()
        customer_id = f"CUST_{random.randint(1001, 5000):04d}"
        order_country = random.choice(['United States', 'Canada', 'Mexico', 'Brazil', 'Argentina', 'UK', 'France', 'Germany', 'China', 'Spain'])
        product_sold_index = random.randint(0,len(product_catalog_rows))  # Tupple
        product_id = product_catalog_rows[product_sold_index-1][2]
        unit_price = product_catalog_rows[product_sold_index-1][8]
        units_sold = random.randint(1, 2000)

        if units_sold > 500:
            discount_applied = random.randint(5, 20) / 100
        else:
            discount_applied = 0 

        total_ammount_paid = (unit_price *(1-discount_applied)) * units_sold

        sales_data.append((transaction_date,
                           transaction_hour,
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

def update_inventory(sales_data:list, pre_sales_inventory:dict):
    aggregated_units_sold = {}

    for record in sales_data[1:]:
        product_id = record[5] 
        units_sold = record[7]    
        # Add units_sold to the existing total for this product, or start a new total
        aggregated_units_sold[product_id] = aggregated_units_sold.get(product_id, 0) + units_sold

    # Inventory Update
    for product_id in aggregated_units_sold:
        if product_id not in pre_sales_inventory.keys():
            pre_sales_inventory[product_id]['final_stock'] =  pre_sales_inventory[product_id]['in_stock'] + pre_sales_inventory[product_id]['new_product'] + pre_sales_inventory[product_id]['returned_product'] + 0
        else:
            units_sold = aggregated_units_sold[product_id]
            pre_sales_inventory[product_id]['units_sold'] = units_sold * -1
            pre_sales_inventory[product_id]['final_stock'] =  pre_sales_inventory[product_id]['in_stock'] + pre_sales_inventory[product_id]['new_product'] + pre_sales_inventory[product_id]['returned_product'] + (units_sold *-1)

    # Generate a list of tuples for updated inventory
    updated_inventory = []
    headers = ('inventory_date','inventory_hour','product_id','in_stock', 'new_product', 'returned_product', 'units_sold', 'final_stock') 
    updated_inventory.append(headers)

    for product_id in pre_sales_inventory.keys():
        values = tuple([v for v in pre_sales_inventory[product_id].values()][0:2] +
                       [product_id] + 
                       [v for v in pre_sales_inventory[product_id].values()][2:])
        # Aggreagate final stock if no sells
        if len(values) == 7:
            in_stock = values[3]
            new_product = values[4]
            returned_product = values[5]
            units_sold = values[6]
            final_stock = in_stock + new_product + returned_product + units_sold
            values = values + (final_stock,)
        else:
            pass
        
        updated_inventory.append(values)
    
    return updated_inventory
        
def upload_tuples_to_gcs_as_csv(
    storage_bucket,
    bucket_instance, 
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
        print(f"Successfully uploaded data to gs://{storage_bucket}/{destination_blob_name}")

    except Exception as e:
        print(f"An error occurred while uploading {destination_blob_name}: {e}")
