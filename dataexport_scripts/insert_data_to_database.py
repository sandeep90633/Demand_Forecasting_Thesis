import psycopg2
import pandas as pd
import numpy as np

def data_ingestion(input_file):
    try:
        df = pd.read_csv(input_file)
        print(f"Successfully imported data file: {input_file}")
        return df
    except Exception as e:
        print(f'Error:{e}, occurred when reading data.')
        raise Exception
    
def inventory_data_cleaning(df):
    #Cleaning dataset
    inventory = df.dropna(how='all')
    inventory.columns = [x.lower() for x in inventory.columns]
    inventory.columns = inventory.columns.str.strip()
    inventory.columns = inventory.columns.str.replace(' ', '_')
    inventory.columns = inventory.columns.str.replace(r'\(.*?\)', '', regex=True).str.replace(r'_$', '', regex=True).str.strip()
    
    return inventory

def clean_inventory_data_values_with_double_quotes(list_of_columns, df):
    
    for col in list_of_columns:
        df[col] = df[col].str.replace(r'[^0-9.]', '', regex=True)
        df[col] = df[col].replace('',np.nan)
        df[col] = df[col].astype(float)
        
        print(f"Modifications are done for this column:{col}")
    
    return df

def inventory_data_transformations_and_local_export(df, local_file_path):
    
    df['current_inventory_quantity'] =  df['current_inventory_quantity'].apply(lambda x: round(x/10  if x >= 100 else x))

    df['total_value'] = round((df['current_inventory_quantity'].fillna(0) * df['cost_per_sku'].fillna(0)))
    
    df.to_csv(local_file_path,index=False)
    
    return df

def sales_data_cleaning_and_export_to_local(df, local_file_path):
    
    #Cleaning dataset before inserting data into database
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
    df['order_date'] = pd.to_datetime(df['order_date'], format='%d-%b-%y')
    
    df.to_csv(local_file_path, index=False)

def connection_and_insert_data(connection, local_file_path, table_name):
    try:
        with open(local_file_path, 'r') as f:
            cursor = connection.cursor()
            
            cursor.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV HEADER",
                f
            )
            connection.commit()
            print(f"Successfully inserted data into the table:{table_name}")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()

    finally:
        cursor.close()
        connection.close()
        
def inventory_data_db_insert(source_data, conn, db_table_name):
    
    # raw data which is 'data/orginal_inventory.csv'
    inventory = data_ingestion(source_data)
    
    inventory = inventory_data_cleaning(inventory)
    
    columns = ['total_value','unit_price','cost_per_sku']

    inventory = clean_inventory_data_values_with_double_quotes(columns, inventory)
    
    local_file_path = 'data/inventory.csv' 
    
    # cleaned and transformed data will be stored in local_file_path, which is    
    inventory = inventory_data_transformations_and_local_export(inventory, local_file_path)
    
    connection_and_insert_data(conn, local_file_path, db_table_name)

def sales_data_db_insert(source_data, conn, db_table_name):
    
    sales = data_ingestion(source_data)
    
    local_file_path = 'data/sales_data.csv'
    
    sales = sales_data_cleaning_and_export_to_local(sales, local_file_path)
    
    connection_and_insert_data(conn, local_file_path, db_table_name)
    
def main():
    
    connection = psycopg2.connect(
    dbname="thesis",
    user="postgres",
    password="*****",
    host="127.0.0.1",
    port="5433"
    )
    
    connection_and_insert_data(connection, 'data/predicted_data.csv', 'weekly_predicted_data')
    # inventory_data_db_insert('data/original_inventory.csv', connection, 'inventory')
    
if __name__ == "__main__":
    main()