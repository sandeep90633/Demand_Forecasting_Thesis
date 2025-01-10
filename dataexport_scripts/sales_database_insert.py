import psycopg2
import pandas as pd

connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="*****",
    host="127.0.0.1",
    port="5433"
)

data = 'data/original_sales_data.csv'

try:
    sales = pd.read_csv(data)
except Exception as e:
    print(f'Error:{e}, occurred when reading data.')
    raise Exception

#Cleaning dataset before inserting data into database
sales = sales.loc[:, ~sales.columns.str.startswith('Unnamed')]
sales['order_date'] = pd.to_datetime(sales['order_date'], format='%d-%b-%y')

sales.to_csv('data/sales_data.csv',index=False)

try:
    with open('data/sales_data.csv', 'r') as f:
        cursor = connection.cursor()
        
        cursor.copy_expert(
            "COPY sales FROM STDIN WITH CSV HEADER",
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