import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

def generate_sales_data(num_records=1000000):
    """Generate 1M sales records for testing"""
    data = []
    
    for _ in range(num_records):
        record = {
            'order_id': fake.uuid4(),
            'customer_id': random.randint(1, 50000),
            'product_id': random.randint(1, 10000),
            'product_name': fake.word(),
            'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports']),
            'price': round(random.uniform(10, 1000), 2),
            'quantity': random.randint(1, 10),
            'order_date': fake.date_between(start_date='-2y', end_date='today'),
            'customer_age': random.randint(18, 80),
            'customer_city': fake.city(),
            'customer_state': fake.state_abbr()
        }
        data.append(record)
    
    df = pd.DataFrame(data)
    df['total_amount'] = df['price'] * df['quantity']
    return df

# Generate and save data
sales_data = generate_sales_data()
sales_data.to_csv('sales_data.csv', index=False)
sales_data.to_parquet('sales_data.parquet', index=False)
print(f"Generated {len(sales_data)} records")
