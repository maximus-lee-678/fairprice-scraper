import defs
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=(retry_if_exception_type(requests.ConnectionError)))
def download_generic(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response


def get_category_json(write_to_file=True):
    url = "https://website-api.omni.fairprice.com.sg/api/category"

    response = download_generic(url)

    if write_to_file:
        with open(defs.CATEGORY_FILE_JSON, "wb") as file:
            file.write(response.content)

    return response.content


def get_products_by_category_json(category_slug, page=1, write_to_file=True):
    url = "https://website-api.omni.fairprice.com.sg/api/product/v2?category={0}&collectionSlug={0}&collectionType=category&includeTagDetails=true&page={1}&pageType=category&slug={0}&url={0}"
    url = url.format(category_slug, page)

    response = download_generic(url)

    if write_to_file:
        with open(defs.PRODUCT_FILE_JSON, "wb") as file:
            file.write(response.content)

    return response.content
