from datetime import datetime, date, timedelta
import logging
# from airflow import DAG
# from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'kusuma',
    'start_date': datetime(2025, 9, 17, 10, 00)
}

# change logger to stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_item_catalog(n=5000, seed=None):
    import random
    
    base_items = [
        ("Pen", "Stationery"),
        ("Notebook", "Stationery"),
        ("Calculator", "Electronics"),
        ("USB Drive", "Electronics"),
        ("Tape Dispenser", "Stationery"),
        ("Stapler", "Stationery"),
        ("Paper Ream", "Stationery"),
        ("Monitor", "Electronics"),
        ("Mouse", "Electronics"),
        ("Keyboard", "Electronics"),
        ("Desk Lamp", "Office Supplies"),
        ("Laptop Stand", "Office Supplies"),
        ("Charger", "Electronics"),
        ("Whiteboard", "Office Supplies"),
        ("Chair Mat", "Furniture"),
        ("Filing Cabinet", "Furniture"),
        ("Shelf Unit", "Furniture"),
        ("Label Printer", "Electronics"),
        ("Desk Organizer", "Office Supplies"),
        ("Scissors", "Stationery")
    ]
    
    # Expanded brands and modifiers for more variety
    brands = ["Acme", "LogiTech", "StapleCo", "WritePro", "NoteMax", "TechZen", "OfficeGo", "QuickMark", "BrightTools", "Zebra", "Pelican", "Play", "Genius", "OfficeMax", "TechPro", "SmartOffice", "ProTools", "EliteOffice", "QuickFix", "PowerTools", "OfficePlus", "TechGear", "WorkSmart", "OfficeElite"]
    modifiers = ["XL", "Mini", "Pro", "Eco", "Lite", "Deluxe", "Smart", "Premium", "Budget", "Ultra", "Slim", "Basic", "Veg", "Rec", "Max", "Plus", "Elite", "Standard", "Advanced", "Compact", "Heavy", "Light", "Fast", "Slow", "Quick", "Easy", "Tough", "Soft", "Hard", "Flexible", "Rigid"]
    
    # Generate unique items with optional seed for reproducibility
    if seed is not None:
        random.seed(seed)
    else:
        # Use current time as seed for true randomization
        random.seed()
    
    catalog = []
    seen_names = set()

    while len(catalog) < n:
        base_item, category = random.choice(base_items)
        brand = random.choice(brands)
        mod = random.choice(modifiers)
        model_num = f"{random.randint(100,999)}-{random.choice('ABCDE')}"

        name = f"{brand} {base_item} {mod} {model_num}"
        if name in seen_names:
            continue

        seen_names.add(name)

        # Assign price based on category, with randomness
        if category == "Stationery":
            price = round(random.uniform(0.5, 10.0), 2)
        elif category == "Electronics":
            price = round(random.uniform(5.0, 100.0), 2)
        elif category == "Office Supplies":
            price = round(random.uniform(2.0, 50.0), 2)
        elif category == "Furniture":
            price = round(random.uniform(20.0, 500.0), 2)
        else:
            price = round(random.uniform(1.0, 20.0), 2)

        catalog.append({
            "name": name,
            "category": category,
            "unit_value": price
        })

    return catalog

def generate_stock_record():
    import random
    from datetime import date, timedelta
    
    # Generate a much larger item catalog for better diversity
    item_catalog = generate_item_catalog(5000)  # Much larger catalog
    warehouses = ["Stockholm", "Gothenburg", "Malmo", "Uppsala", "Västerås", "Linköping", "Örebro", "Helsingborg", "Jönköping", "Norrköping", "Lund", "Umeå", "Gävle", "Borås", "Södertälje"]
    
    # Generate a single stock record
    current_date = date.today()
    warehouse = random.choice(warehouses)
    
    # Use random sampling to get more diverse item selection
    # This ensures we're not just picking from the same small subset
    item = random.choice(item_catalog)
    
    # Random stock behavior with more variation
    opening_stock = random.randint(10, 500)  # Wider range
    receipts = random.randint(0, 200)  # More variation in receipts
    max_issues = opening_stock + receipts
    issues = random.randint(0, max_issues)
    closing_stock = max(0, opening_stock + receipts - issues)  # Ensure non-negative stock
    
    stock_record = {
        "stock_date": current_date.strftime("%Y-%m-%d"),
        "warehouse_id": warehouse,
        "item_name": item["name"],
        "category": item["category"],
        "opening_stock": opening_stock,
        "receipts": receipts,
        "issues": issues,
        "closing_stock": closing_stock,
        "unit_value": item["unit_value"]
    }
    
    return stock_record

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    import os

    broker_url = os.getenv('BROKER_URL', 'localhost:9092')
    
    producer = KafkaProducer(bootstrap_servers=[broker_url], max_block_ms=5000, api_version=(1, 4, 1))
    curr_time = time.time()

    while True:
        # if time.time() > curr_time + 60: #1 minute
        #     breakpc
        try:
            stock_data = generate_stock_record()
            
            producer.send('warehouse_stock', json.dumps(stock_data).encode('utf-8'))
            logging.info(f"Sent stock data: {stock_data}")
            time.sleep(1)  # Send one record per second
        except Exception as e:
            logging.error(f"Error: {e}")
            continue


if __name__ == "__main__":
    stream_data()
# with DAG('warehouse_stock_automation',
#         default_args=default_args,
#         schedule_interval='@daily',
#         catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_stock_data',
#         python_callable=stream_data
#     )
