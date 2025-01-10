import psycopg2

connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="*****",
    host="127.0.0.1",
    port="5433"
)

data = 'data/sales_data.csv'

try:
    with open(data, 'r') as f:
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