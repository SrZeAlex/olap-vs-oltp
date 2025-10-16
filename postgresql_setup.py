import pandas as pd
from sqlalchemy import create_engine, text
import time

# Connection string - adjust for your setup
postgres_url = "postgresql://postgres:password@localhost:5432/ecommerce"
engine = create_engine(postgres_url)

# Load data into PostgreSQL
df = pd.read_csv('sales_data.csv')
df['order_date'] = pd.to_datetime(df['order_date']) # Convert to datetime objects
start_time = time.time()
df.to_sql('sales', engine, if_exists='replace', index=False, method='multi', chunksize=10000)
load_time = time.time() - start_time

print(f"PostgreSQL load time: {load_time:.2f} seconds")

# Create indexes for better performance
with engine.connect() as conn:
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_order_date ON sales(order_date);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_category ON sales(category);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_customer_state ON sales(customer_state);"))
    conn.commit()
