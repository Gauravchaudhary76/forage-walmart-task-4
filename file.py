import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('shipping_data.db')
cursor = conn.cursor()

# Step 1: Read Spreadsheet 0
spreadsheet_0 = pd.read_csv('spreadsheet_0.csv')
spreadsheet_0.to_sql('Table0', conn, if_exists='replace', index=False)

# Step 2: Read Spreadsheet 1 and Spreadsheet 2
spreadsheet_1 = pd.read_csv('spreadsheet_1.csv')
spreadsheet_2 = pd.read_csv('spreadsheet_2.csv')

# Merge Spreadsheet 1 and 2 based on 'shipping_identifier'
merged_data = pd.merge(spreadsheet_1, spreadsheet_2, on='shipping_identifier')

# Process the merged data to extract product information
shipment_data = []
for _, row in merged_data.iterrows():
    products = row['product_name'].split(',')  # Assuming products are comma-separated
    quantities = list(map(int, row['quantity'].split(',')))
    for product, quantity in zip(products, quantities):
        shipment_data.append({
            'shipping_identifier': row['shipping_identifier'],
            'product_name': product.strip(),
            'quantity': quantity,
            'origin': row['origin'],
            'destination': row['destination']
        })

# Convert to DataFrame and insert into the database
shipment_df = pd.DataFrame(shipment_data)
shipment_df.to_sql('Table1', conn, if_exists='replace', index=False)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data successfully inserted into the database.")
