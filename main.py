import process
import threading
import defs
import get_categories
import pandas_ops
import os
from pathlib import Path

NUM_THREADS = 36


def main():
    print("Starting scrape.")
    lock = threading.Lock()

    Path(defs.PRODUCT_FILE_DUPLICATES_CSV).unlink(missing_ok=True)
    Path(defs.BRAND_FILE_DUPLICATES_CSV).unlink(missing_ok=True)
    Path(defs.PRODUCT_FILE_FINAL_CSV).unlink(missing_ok=True)
    Path(defs.BRAND_FILE_FINAL_CSV).unlink(missing_ok=True)
    Path(defs.COUNTRY_FILE_FINAL_CSV).unlink(missing_ok=True)

    if not os.path.exists(defs.CATEGORY_FILE_FINAL_CSV):
        print("Category file not found, pulling down.")
        slug_list = get_categories.main()
    else:
        slug_list = pandas_ops.read_category_final()
    print("Category slugs found.")

    print("Getting products and brands.")
    process.get_products_brands_from_category_slugs(slug_list, lock, max_threads=NUM_THREADS)
    print("Done downloading products and brands.")

    pandas_ops.drop_duplicates()
    Path(defs.PRODUCT_FILE_DUPLICATES_CSV).unlink(missing_ok=True)
    Path(defs.BRAND_FILE_DUPLICATES_CSV).unlink(missing_ok=True)
    print("Dropped duplicates from products and brands.")

    pandas_ops.generate_country_mappings()
    print("Generated country mappings and updated products to use ids.")

    print("Finished.")


if __name__ == "__main__":
    main()
