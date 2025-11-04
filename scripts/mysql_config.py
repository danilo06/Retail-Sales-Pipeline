#Import libraries
import mysql.connector
import requests
import os

#Connect to sales database
connection = mysql.connector.connect(
    user='admin321', 
    password='admin321', 
    host='127.0.0.1',
    port=3310,
    database='sales')

cursor = connection.cursor()

#Download SQL file to populate the database
sql_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/sales.sql"
file_path = "./mysql/files/sales.sql"

print("📥 Downloading SQL file...")
response = requests.get(sql_url)
response.raise_for_status()
with open(file_path, "wb") as f:
    f.write(response.content)
print(f"✅ SQL file downloaded: {file_path}")

print("🚀 Executing SQL script...")
with open(file_path, "r", encoding="utf-8") as f:
    sql_commands = f.read()
cursor.execute("DROP DATABASE IF EXISTS sales;")
cursor.execute("CREATE DATABASE sales;")
cursor.execute("USE sales;")
print("✅ Database 'sales' recreated.")
for command in sql_commands.split(";"):
    command = command.strip()
    if command:
        try:
            cursor.execute(command)
        except mysql.connector.Error as err:
            print(f"⚠️ Skipped command due to error: {err}")

connection.commit()
print("✅ Database successfully populated!")

cursor.execute("SELECT COUNT(*) FROM sales_data;")
row_count = cursor.fetchone()[0]
print(f"📊 Rows imported into sales_data: {row_count}")

cursor.close()
connection.close()