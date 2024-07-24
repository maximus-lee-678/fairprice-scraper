🧺 FairPrice Scraper
=====================
| Quick Python script that pulls ALL products, brands, and categories from FairPrice's API endpoints.
|
- Run main.py to get started. Categories are retrieved only once by default.
- If you want to refresh the categories list, run get_categories.py.
|
- Since there is overlap for products in different categories, the scraper first saves all duplicates before dropping them using pandas. 
- "description" and "key_information" have had their newline characters replaced with the "\\n" literal to support CSV export.
|
- Information you want not being extracted? Inspect the "Products" endpoint in your preferred API platform or browser and add more keys to get_products_brands_from_one_category_slug() in process.py.
|
- Multithreading factor can be modified in main.py. Note that if the main process is terminated via Interrupt, the threads will still run to completion unless you kill the Python service itself.

⚡ Used Endpoints
------------------
1. Categories (Main and Sub-categories)

.. code-block:: console

  https://website-api.omni.fairprice.com.sg/api/category

2. Products (and Brands)
  - {0} - slug obtained from categories endpoint
  - {1} - page number

.. code-block:: console

  https://website-api.omni.fairprice.com.sg/api/product/v2?category={0}&collectionSlug={0}&collectionType=category&includeTagDetails=true&page={1}&pageType=category&slug={0}&url={0}

🚫 Unused Endpoints
--------------------
1. Brands (reason: only returns maximum 100 brands)

.. code-block:: console

  https://website-api.omni.fairprice.com.sg/api/brand
