import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

# Define the PostgreSQL connection parameters
db_params = {
    'dbname': "postgres",
    'user': "postgres",
    'password': "Zeb170cr@1999",
    'host': "localhost",
    'port': "5432"
}

# Defining schema and table name
table_name = 'csci_765.DB_Assignment7'

# Defining tuple with the values in it
fruits = ("apple", "banana", "cherry")

# displays index[1] from the tuple fruits
print("value of element at 1 index from fruits is : " + fruits[1])

# Reads the train.csv from the given path
train = pd.read_csv("C:/Users/achut/Downloads/train.csv")

# Display the first 10 rows of the DataFrame
print(train.head(10))

#  histogram of the 'LoanAmount' column using bins = 50
train['LoanAmount'].hist(bins=50)

# connecting to the PostgreSQL database on local
conn = psycopg2.connect(**db_params)

# Cursor object pg_db_cur
pg_db_cur = conn.cursor()

# DataFrame to a list of tuples convertion using .tolist()
list_of_tuples = train.values.tolist()

# Insert data into the PostgreSQL table
insert_query = f"INSERT INTO {table_name} VALUES %s"

# Use the execute_values method to insert multiple rows efficiently
execute_values(pg_db_cur, insert_query, list_of_tuples)

# Commit the changes to the database
conn.commit()

# Close the cursor
pg_db_cur.close()
# Close the and connection
conn.close()