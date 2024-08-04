import defs
from pathlib import Path
import pandas as pd

def read_category_final():
    df_categories = pd.read_csv(defs.CATEGORY_FILE_FINAL_CSV)
    return df_categories['url'].tolist()


def drop_duplicates():
    df_product = pd.read_csv(defs.PRODUCT_FILE_DUPLICATES_CSV)
    df_brand = pd.read_csv(defs.BRAND_FILE_DUPLICATES_CSV)

    df_product = df_product.drop_duplicates(subset="id")
    df_brand = df_brand.drop_duplicates(subset="id")

    df_product.to_csv(defs.PRODUCT_FILE_FINAL_CSV, index=False)
    df_brand.to_csv(defs.BRAND_FILE_FINAL_CSV, index=False)

def generate_country_mappings():
    # create country csv
    df_product = pd.read_csv(defs.PRODUCT_FILE_FINAL_CSV)
    countries = df_product["country"].unique()

    df_country = pd.DataFrame(countries, columns=["country"])
    df_country.index += 1
    df_country = df_country.reset_index(names="id")

    df_country.to_csv(defs.COUNTRY_FILE_FINAL_CSV, index=False)

    # rename column and insert at old position
    df_id_country_named = df_country.rename(columns={"id": "id_country"})

    df_product = df_product.merge(df_id_country_named, how='left', on="country")
    product_columns = list(df_product.columns)
    country_column_index = product_columns.index("country")
    product_columns.insert(country_column_index + 1, product_columns.pop(product_columns.index("id_country")))  # add after country then remove og
    df_product = df_product[product_columns]
    df_product = df_product.drop(columns=["country"])

    Path(defs.PRODUCT_FILE_FINAL_CSV).unlink(missing_ok=True)
    df_product.to_csv(defs.PRODUCT_FILE_FINAL_CSV, index=False)
