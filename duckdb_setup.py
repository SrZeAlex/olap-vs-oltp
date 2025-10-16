import duckdb
import time

# Connect to DuckDB
conn = duckdb.connect('ecommerce.duckdb')

# Load data into DuckDB
start_time = time.time()
conn.execute("CREATE TABLE sales AS SELECT * FROM 'sales_data.parquet'")
load_time = time.time() - start_time

print(f"DuckDB load time: {load_time:.2f} seconds")
