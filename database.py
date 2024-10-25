import pandas as pd
import sqlite3

# Database connection setup
db_connection = sqlite3.connect('walmart_shipping_data.db')
cursor = db_connection.cursor()

# Step 1: Load the data from spreadsheets
spreadsheet_0 = pd.read_excel('spreadsheet_0.xlsx')
spreadsheet_1 = pd.read_excel('spreadsheet_1.xlsx')
spreadsheet_2 = pd.read_excel('spreadsheet_2.xlsx')

# Function to insert data into the database
def insert_data(table, data):
    placeholders = ', '.join(['?' for _ in data[0]])
    query = f"INSERT INTO {table} VALUES ({placeholders})"
    cursor.executemany(query, data)

# Step 2: Insert data from spreadsheet 0 into the database
spreadsheet_0_data = spreadsheet_0.values.tolist()
insert_data('products', spreadsheet_0_data)

# Step 3: Merge and transform data from spreadsheets 1 and 2
# Merging data on 'shipping_id' to get origin and destination for each shipment
merged_data = pd.merge(spreadsheet_1, spreadsheet_2, on='shipping_id')

# Creating rows for each product in the shipment
shipment_rows = []
for index, row in merged_data.iterrows():
    product_id = row['product_id']
    shipping_id = row['shipping_id']
    quantity = row['quantity']
    origin = row['origin']
    destination = row['destination']
    shipment_rows.append((shipping_id, product_id, quantity, origin, destination))

# Step 4: Insert shipment data into the database
insert_data('shipments', shipment_rows)

# Step 5: Commit changes and close the database connection
db_connection.commit()
cursor.close()
db_connection.close()

print("Database population completed successfully.")
