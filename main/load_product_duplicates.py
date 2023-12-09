import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Sample DataFrame (replace this with your actual DataFrame)

df = pd.read_csv('combined_results.csv', sep=';')
df['created_at'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
df['updated_at'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
df.rename(columns={'products_id': 'title'}, inplace=True)

df_product_duplicates = df[['title', 'created_at', 'updated_at']]
df_product_duplicates.drop_duplicates(inplace=True)
df_product_duplicates.dropna(inplace=True)
# print(df_product_duplicates)

# MySQL database connection parameters
db_connection_string = "mysql+mysqlconnector://user:pwd@localhost:3306/db"
engine = create_engine(db_connection_string)

# Replace 'your_table' with the actual table name in your MySQL database
table_name = 'product_duplicates'

# Insert data into MySQL database
df_product_duplicates.to_sql(name=table_name, con=engine, if_exists='append', index=False)
