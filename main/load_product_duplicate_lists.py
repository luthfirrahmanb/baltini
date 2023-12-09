import pandas as pd
import mysql.connector
from datetime import datetime
from sqlalchemy import create_engine

# Connect to your MySQL database
database_config = {
    'host': 'host',
    'user': 'user',
    'password': 'password',
    'database': 'db',
    'auth_plugin': 'caching_sha2_password'
}
connection = mysql.connector.connect(**database_config)

df_final = pd.DataFrame()

def extract_data():
    query =  """
        select * from product_duplicates
"""
    df = pd.read_sql(query, connection)
    return df

df_product_duplicates = extract_data()
df_product_duplicates.rename(columns={'id': 'product_duplicate_id'}, inplace=True)
df_product_duplicates.drop(['created_at', 'updated_at'], axis=1, inplace=True)

df = pd.read_csv('combined_results.csv', sep=';')
df['created_at'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
df['updated_at'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

# for x in df_product_duplicates['title'].tolist():
#     # df_final = df_final.append(df[df['products_id'] == x])
#     df_final = pd.concat([df_final, df[df['products_id'] == x]], ignore_index=True)
#     # df

# df[df.similar_products_id == '2ed65a2f-1a69-4d41-aa3e-7fdc4f97f0e0'].to_csv('check.csv', index=False, sep=';')

df.rename(columns={'products_id': 'title', 'similar_products_id': 'product_id', 'products_external_id': 'external_id'}, inplace=True)
df = df[['title', 'product_id', 'external_id', 'created_at', 'updated_at', 'similarity']]

df = df.merge(df_product_duplicates, on='title')
df['deleted_at'] = None


df = df[['product_duplicate_id', 'external_id', 'product_id', 'deleted_at', 'created_at', 'updated_at', 'title']]

df[df.title == '2ed65a2f-1a69-4d41-aa3e-7fdc4f97f0e0'].to_csv('check.csv', index=False, sep=';')

# MySQL database connection parameters
db_connection_string = "mysql+mysqlconnector://user:pswd@localhost:3306/db"
engine = create_engine(db_connection_string)

# Replace 'your_table' with the actual table name in your MySQL database
table_name = 'product_duplicate_lists'


for x in df_product_duplicates['title'].tolist():
    try:
        # Insert data into MySQL database
        df[df.title == x][['product_duplicate_id', 'external_id', 'product_id', 'deleted_at', 'created_at', 'updated_at']].to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except BaseException as e:
        continue
