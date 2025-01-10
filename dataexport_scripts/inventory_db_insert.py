import psycopg2
import pandas as pd
import numpy as np

connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Dinu@123",
    host="127.0.0.1",
    port="5433"
)

inventory = 'data/original_inventory.csv'

try:
    inventory = pd.read_csv(inventory)
except Exception as e:
    print(f'Error:{e}, occurred when reading data.')
    raise Exception

#Cleaning dataset
inventory.columns = [x.lower() for x in inventory.columns]
inventory.columns = inventory.columns.str.strip()
inventory.columns = inventory.columns.str.replace(' ', '_')
inventory.columns = inventory.columns.str.replace(r'\(.*?\)', '', regex=True).str.replace(r'_$', '', regex=True).str.strip()

def clean_values_with_double_quotes(list_of_columns, df):
    
    for col in list_of_columns:
        df[col] = df[col].str.replace(r'[^0-9.]', '', regex=True)
        df[col] = df[col].replace('',np.nan)
        df[col] = df[col].astype(float)
        
        print(f"Modifications are done for this column:{col}")
    
    return df

columns = ['total_value','unit_price','cost_per_sku']

inventory = clean_values_with_double_quotes(columns, inventory)

inventory.to_csv('data/inventory.csv',index=False)

try:
    with open('data/inventory.csv', 'r') as f:
        cursor = connection.cursor()
        
        cursor.copy_expert(
            "COPY inventory FROM STDIN WITH CSV HEADER",
            f
        )
        connection.commit()
        print("Successfully inserted data into the table.")

except Exception as e:
    print(f"Error: {e}")
    connection.rollback()

finally:
    cursor.close()
    connection.close()