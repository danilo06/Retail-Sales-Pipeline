import psycopg2
import requests
import os
import pandas as pd
from sqlalchemy import create_engine

# Connect to sales database
connection = psycopg2.connect(
   database='sales', 
   user='admin321',
   password='admin321',
   host='127.0.0.1', 
   port=5435
)

# Download the csv file to populate the table
csv_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/sales-csv3mo8i5SHvta76u7DzUfhiw.csv"
csv_path = "./postgres/files/sales.csv"

print("📥 Downloading CSV file...")
response = requests.get(csv_url)
response.raise_for_status()
with open(csv_path, "wb") as f:
    f.write(response.content)
print(f"✅ CSV file downloaded: {csv_path}")

cursor = connection.cursor()

# Create the sales_data table
SQL = """DROP TABLE IF EXISTS sales_data;
CREATE TABLE IF NOT EXISTS sales_data(
    rowid INTEGER PRIMARY KEY NOT NULL,
    product_id INT NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    price DECIMAL(10,2) DEFAULT 0.0 NOT NULL,
    quantity INTEGER NOT NULL,
    timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);"""
cursor.execute(SQL)
print("Table created")

# ✅ Load data into sales_data table using COPY
with open(csv_path, 'r', encoding='utf-8') as f:
    cursor.copy_expert("""
        COPY sales_data (rowid, product_id, customer_id, price, quantity, timestamp)
        FROM STDIN WITH CSV HEADER
    """, f)
connection.commit()
print("✅ CSV loaded into sales_data table")

cursor.execute("SELECT COUNT(*) FROM sales_data;")
row_count = cursor.fetchone()[0]
print(f"📊 Rows imported: {row_count}")

cursor.close()
connection.close()