# Import libraries required for connecting to MySQL and PostgreSQL
import mysql.connector
import psycopg2

# Connect to MySQL
connection_sql = mysql.connector.connect(
    user='admin321', 
    password='admin321', 
    host='127.0.0.1',
    port=3310,
    database='sales')

# Connect to PostgreSql
connection_postgres = psycopg2.connect(
   database='sales', 
   user='admin321',
   password='admin321',
   host='127.0.0.1', 
   port= 5435
)

# Find out the last rowid from PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data in PostgreSQL
def get_last_rowid():
    with connection_postgres.cursor() as cursor_postgres:
        cursor_postgres.execute("SELECT MAX(rowid) FROM sales_data")  
        result = cursor_postgres.fetchone()[0] or 0
    return result

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than 
# the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    cursor_sql = connection_sql.cursor()
    cursor_sql.execute("SELECT * FROM sales_data WHERE rowid > %s", (rowid,))
    result = cursor_sql.fetchall()
    return result                       

new_records = get_latest_records(last_row_id)

print("New rows on staging data warehouse = ", len(new_records))

# Insert the additional records from MySQL into PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the 
# sales_data table in database PostgreSql.

def insert_records(records):
    if not records:
        return
    with connection_postgres.cursor() as cursor_pg:
        cursor_pg.execute("SELECT product_id, price FROM sales_data WHERE price IS NOT NULL")
        rows = cursor_pg.fetchall()
        existing_prices = {r[0]: float(r[1]) for r in rows} if rows else {}

        if existing_prices:
            default_price = sum(existing_prices.values()) / len(existing_prices)
        else:
            default_price = 0.0
            print("⚠️ WARNING: no existing prices found in sales_data, using fallback price 0.0")

        records_to_insert = []
        for r in records:
            rowid = r[0]
            product_id = r[1]
            customer_id = r[2]
            quantity = r[3]
            price = existing_prices.get(product_id, default_price)
            records_to_insert.append((rowid, product_id, customer_id, price, quantity))

        insert_sql = """
            INSERT INTO sales_data (rowid, product_id, customer_id, price, quantity)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (rowid) DO NOTHING
        """
        cursor_pg.executemany(insert_sql, records_to_insert)

    connection_postgres.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))
# disconnect from MySQL warehouse
connection_sql.close()
# disconnect from PostgreSQL data warehouse 
connection_postgres.close()
# End of program
