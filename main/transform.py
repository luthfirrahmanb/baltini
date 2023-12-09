import pandas as pd
from tqdm import tqdm, tqdm_pandas
from rapidfuzz import fuzz
from extract import extract_data

tqdm_pandas(tqdm())

# Sample data
# data = {
#     'product_id': [1, 2, 3, 4],
#     'product_name': ['Louis Vuitton Bag', 'Louis Vuiton Shoe', 'Gucci Shirt', 'Prada Sunglasses']
# }

df = extract_data()

# Get Only the Material data from products_tags
# wbd -> will be deleted
df[['wbd', 'clean']] = df['products_tags'].str.split("Material:", expand=True)
df.drop('wbd', axis=1, inplace=True)

df[['material', 'wbd']] = df['clean'].str.split("Origin:", expand=True)
df.drop('wbd', axis=1, inplace=True)
df.drop('products_tags', axis=1, inplace=True)
df['product_name'] = df['product_name'] + '|' + df['material'].str.lower()
df['product_name'] = df['product_name'].str.strip()
df.drop('clean', axis=1, inplace=True)
df.drop('material', axis=1, inplace=True)
df['product_name'] = df['product_name'].astype(str)

df_brands = df['groupies'].drop_duplicates()

# Create an empty DataFrame to store the combined results
combined_df = pd.DataFrame()

# Function to compare similarity between product names within the same brand
def compare_similarity(row, brand):
    current_product_name = row['product_name']
    similarities = df[(df['products_id'] != row['products_id']) & (df.groupies == brand)].progress_apply(
        lambda x: pd.Series({
            'similar_products_id': x['products_id'],
            'similar_product_name': x['product_name'],
            'similar_products_external_id': x['products_external_id'],
            'similarity': fuzz.ratio(current_product_name, x['product_name'])
        }),
        axis=1
    )
    return similarities.to_dict(orient='records')

# Apply the function to create a new column 'similarities' for each brand
for brand in df_brands:
    df_subset = df[df['groupies'] == brand].copy()
    df_subset['similarities'] = df_subset.apply(compare_similarity, brand=brand, axis=1)
    
    # Unpack the 'similarities' column to separate rows
    df_subset = df_subset.explode('similarities')

    # Expand the 'similarities' column into separate columns
    df_subset = pd.concat([df_subset.drop(['similarities'], axis=1), df_subset['similarities'].apply(pd.Series)], axis=1)

    # Display the results for each brand
    print(f"Similarity Matrix for {brand}:")
    print(df_subset)
    
    # Concatenate the results to the combined DataFrame
    # df_subset.dropna(inplace=True)
    combined_df = pd.concat([combined_df, df_subset], ignore_index=True)

# combined_df.dropna(inplace=True)
# Save the combined results to a CSV file
combined_df.to_csv('combined_results.csv', index=False, sep=';')
