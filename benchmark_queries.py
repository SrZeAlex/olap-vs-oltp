import time
import pandas as pd
from sqlalchemy import create_engine
import duckdb

def time_query(connection, query, db_type):
    """Time query execution"""
    start_time = time.time()
    
    if db_type == 'postgresql':
        result = pd.read_sql(query, connection)
    else:  # duckdb
        result = connection.execute(query).df()
    
    end_time = time.time()
    return end_time - start_time, len(result)

# Test queries
queries = {
    "Aggregation by Category": """
        SELECT category, 
               COUNT(*) as order_count,
               SUM(total_amount) as total_revenue,
               AVG(total_amount) as avg_order_value
        FROM sales 
        GROUP BY category 
        ORDER BY total_revenue DESC
    """,
    
    "Time Series Analysis": """
        SELECT DATE_TRUNC('month', order_date) as month,
               SUM(total_amount) as monthly_revenue,
               COUNT(DISTINCT customer_id) as unique_customers
        FROM sales
        WHERE order_date >= '2023-01-01'
        GROUP BY DATE_TRUNC('month', order_date)
        ORDER BY month
    """,
    
    "Customer Segmentation": """
        SELECT customer_state,
               customer_age / 10 * 10 as age_group,
               COUNT(*) as orders,
               AVG(total_amount) as avg_order_value
        FROM sales
        WHERE total_amount > 100
        GROUP BY customer_state, customer_age / 10 * 10
        HAVING COUNT(*) > 100
        ORDER BY orders DESC
    """,
    
    "Complex Analytical Query": """
        SELECT category,
               customer_state,
               COUNT(*) as orders,
               SUM(total_amount) as revenue,
               AVG(price) as avg_price,
               MAX(total_amount) as max_order,
               COUNT(DISTINCT customer_id) as unique_customers
        FROM sales
        WHERE order_date BETWEEN '2023-06-01' AND '2023-12-31'
        GROUP BY category, customer_state
        HAVING SUM(total_amount) > 10000
        ORDER BY revenue DESC
        LIMIT 20
    """
}

# PostgreSQL connection
postgres_engine = create_engine("postgresql://postgres:password@localhost:5432/ecommerce")

# DuckDB connection  
duck_conn = duckdb.connect('ecommerce.duckdb')

# Run benchmarks
results = []

for query_name, query in queries.items():
    print(f"\\nTesting: {query_name}")
    
    # PostgreSQL
    pg_time, pg_rows = time_query(postgres_engine, query, 'postgresql')
    print(f"PostgreSQL: {pg_time:.3f} seconds ({pg_rows} rows)")
    
    # DuckDB
    duck_time, duck_rows = time_query(duck_conn, query, 'duckdb')
    print(f"DuckDB: {duck_time:.3f} seconds ({duck_rows} rows)")
    
    # Calculate speedup
    speedup = pg_time / duck_time if duck_time > 0 else 0
    print(f"DuckDB Speedup: {speedup:.2f}x")
    
    results.append({
        'query': query_name,
        'postgresql_time': pg_time,
        'duckdb_time': duck_time,
        'speedup': speedup,
        'rows': pg_rows
    })

# Create summary report
summary_df = pd.DataFrame(results)
summary_df.to_csv('performance_comparison.csv', index=False)
print("\\nBenchmark results saved to performance_comparison.csv")