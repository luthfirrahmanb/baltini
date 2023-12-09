import pandas as pd
import mysql.connector
# from fuzzywuzzy import fuzz

# Connect to your MySQL database
database_config = {
    'host': 'host',
    'user': 'user',
    'password': 'password',
    'database': 'db',
    'auth_plugin': 'caching_sha2_password'
}
connection = mysql.connector.connect(**database_config)

def extract_data():
    # Fetch data from your suppliers (replace this with your actual query)
    query =  """
        with temp1 as (
 select 
p.id products_id
, p.tags products_tags
, p.external_id products_external_id
, b.name as brands
, lower(s.name) seasons
, lower(p.category) categories
, lower(p.gender) gender
, lower(p.external_color_code) color_code
, replace(concat(lower(p.title), '|',lower(b.name), '|', lower(s.name), '|', lower(p.category), '|',lower(p.gender), '|',lower(p.external_color_code)), ' ', '') product_name
, concat(lower(b.name), '|', lower(s.name), '|', lower(p.category), '|',lower(p.gender), '|',lower(p.external_color_code)) groupies
from products p
inner join brands b on b.id = p.brand_id
inner join seasons s on s.id = p.season_id
where b.name in ('DOLCE & GABBANA EYEWEAR', 'A-COLD-WALL*', '6397'
'424', '23° EYEWEAR', 'CASIO G-SHOCK', '19:13 DRESSCODE')
-- where title like '%Polo Ralph Lauren shirt%'
-- and category like '%clothing%' and gender like 'men%'
-- and external_color_code like 'green%'
-- and tags like '%cotton%' 
)
select * from temp1 where product_name is not null
"""
    df = pd.read_sql(query, connection)
    return df

# extract_data()