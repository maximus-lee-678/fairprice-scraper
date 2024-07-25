import defs
import download
import os
import json
import math
import threading
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import time


def get_all_categories(category_dict):
    def recursively_extract_subcategories(inspection_point, category_info, category_id_list):
        # might have, might not
        if "subCategories" not in inspection_point:
            return

        for category in inspection_point["subCategories"]:
            if category.get("id") not in category_id_list:
                category_info.append({
                    "id": category.get("id"),
                    "name": category.get("name"),
                    "image_url": category.get("image"),
                    "url": category.get("slug"),
                    "created_at": category.get("createdAt"),
                    "updated_at": category.get("updatedAt"),
                    "updated_by": category.get("updatedBy")
                })
                category_id_list.append(category.get("id"))

                # im going deeper, leo
                recursively_extract_subcategories(category, category_info, category_id_list)

    category_json_all = json.loads(category_dict)
    category_info = []
    category_id_list = []

    for category in category_json_all["data"]["category"]:
        if category.get("id") not in category_id_list:
            category_info.append({
                "id": category.get("id"),
                "name": category.get("name"),
                "image_url": category.get("image"),
                "url": category.get("slug"),
                "created_at": category.get("createdAt"),
                "updated_at": category.get("updatedAt"),
                "updated_by": category.get("updatedBy")
            })
            category_id_list.append(category.get("id"))

            recursively_extract_subcategories(category, category_info, category_id_list)

    df = pd.DataFrame(category_info)
    df.to_csv(defs.CATEGORY_FILE_FINAL_CSV, index=False)

    return category_info


def fragment_list(min_slice_count, max_item_size, item_list):
    total_item_count = len(item_list)

    # if cannot fit, still spread evenly, find minimum number of slices needed
    if max_item_size * min_slice_count < total_item_count:
        num_slices_to_create = math.ceil(total_item_count / max_item_size)
    else:
        num_slices_to_create = min_slice_count

    slice_count_list = []
    num_items_left = total_item_count
    items_per_slice = math.floor(total_item_count / num_slices_to_create)

    for i in range(0, num_slices_to_create):
        slice_count_list.append(items_per_slice)
        num_items_left -= items_per_slice

    if num_items_left > 0:    # distribute leftover items, since value was floored above
        # guaranteed to never exceed number of slice, it would increment during earlier items_per_slice value calculation
        for i in range(0, num_items_left):
            slice_count_list[i] += 1

    while 0 in slice_count_list:    # prune empty slices if total_item_count < min_slice_count
        slice_count_list.remove(0)

    # print(f"Partition Plan: {total_item_count} -> {slice_count_list}")

    items_processed = 0
    list_of_lists = []
    for num_items in slice_count_list:
        list_of_lists.append(item_list[items_processed:items_processed + num_items])
        items_processed += num_items

    return list_of_lists


def get_products_brands_from_category_slugs(slug_list, lock, max_threads):
    def scrape_thread(thread_id, slug_sublist):
        print(f"""[THREAD/{thread_id}] starting!""")
        for i, category_slug in enumerate(slug_sublist):
            get_products_brands_from_one_category_slug(thread_id, category_slug, lock)
            print(f"[THREAD/{thread_id}] Finished processing {category_slug} ({i + 1}/{len(slug_sublist)}).")
        print(f"""[THREAD/{thread_id}] stopping!""")

    fragmented_list = fragment_list(min_slice_count=max_threads, max_item_size=10, item_list=slug_list)

    threads_created = 0
    displayed_active_thread_count = 0   # used to not spam stdout with currently active threads every second
    has_started = False
    num_threads_to_run = len(fragmented_list)

    threads = []

    print(f"[MAIN] Starting up to {max_threads} threads.")
    while len(threads) != 0 or not has_started:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

        if threads_created < num_threads_to_run and len(threads) < max_threads:
            print(f"[MAIN] Threads Active before addition: {len(threads)}")

            # minimum of how many threads left to create or how much the "buffer" can fit
            # in this loop, i can be treated as thread id to create and is zero-based
            for i in range(threads_created, min(num_threads_to_run, threads_created + (max_threads - len(threads)))):
                thread = threading.Thread(target=scrape_thread, args=(i, fragmented_list[i]))
                threads.append(thread)
                thread.start()
                print(f"[MAIN] Thread ID {threads_created} created.")

                threads_created += 1

            print(f"[MAIN] Threads Active after addition: {len(threads)}")
            displayed_active_thread_count = len(threads)

        elif displayed_active_thread_count != len(threads):
            # only show updates if there's changes
            print(f"[MAIN] Threads Currently Active: {len(threads)}")
            displayed_active_thread_count = len(threads)

        if not has_started:
            has_started = True

        time.sleep(1)


def get_products_brands_from_one_category_slug(thread_id, category_slug, lock):
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=(retry_if_exception_type(KeyError)))
    def get_products_json(slug, working_page):
        products_json = json.loads(download.get_products_by_category_json(slug, page=working_page, write_to_file=False))

        if not products_json.get("data"):
            raise KeyError(f"""[THREAD/{thread_id}] No "data" key in payload for {slug}.""")
        if not products_json["data"]["count"] == 0 and not products_json.get("data").get("product"):
            raise KeyError(f"""[THREAD/{thread_id}] No "product" key in payload for {slug}.""")

        return products_json

    def recursively_extract_parent_categories(inspection_point, parent_category_id_list):
            if inspection_point.get("parentCategory") is None:
                return

            parent_category_id_list.append(inspection_point.get("parentCategory").get("id"))

            # im going deeper, leo
            recursively_extract_parent_categories(inspection_point.get("parentCategory"), parent_category_id_list)

    working_page = 1
    while True:
        products_json = get_products_json(category_slug, working_page)

        # some categories are empty, skip processing entirely
        if products_json["data"]["count"] == 0:
            break

        product_info = []
        brand_info = []

        for product in products_json["data"]["product"]:
            product_id = product.get("id")
            brand_id = product.get("brand").get("id")
            parent_categories_asc = []
            recursively_extract_parent_categories(product.get("primaryCategory"), parent_categories_asc)

            product_info.append({
                "id": product_id,
                "name": product.get("name"),
                "price": product.get("final_price"),
                "display_unit": product.get("metaData").get("DisplayUnit"),
                "description": product.get("description").replace("\n", "\\n") if product.get("description") else None,
                "key_information": product.get("metaData").get("Key Information").replace("\n", "\\n") if product.get("metaData").get("Key Information") else None,
                "image_url": product.get("images")[0] if product.get("images") else None,
                "id_brand": brand_id,
                "id_category_primary": product.get("primaryCategory").get("id"),
                "id_category_parent_major": str(parent_categories_asc[1]) if len(parent_categories_asc) == 2 else str(parent_categories_asc[0]),
                "id_category_parent_minor": str(parent_categories_asc[0]) if len(parent_categories_asc) == 2 else None,
                "id_category_secondary": ",".join([str(id) for id in product.get("secondaryCategoryIds")]) if product.get("secondaryCategoryIds") else None,
                "country": product.get("metaData").get("Country of Origin"),
                # reviews can be null
                "rating_1": product.get("reviews").get("statistics").get("distribution")[4].get("count") if product.get("reviews") else 0,
                "rating_2": product.get("reviews").get("statistics").get("distribution")[3].get("count") if product.get("reviews") else 0,
                "rating_3": product.get("reviews").get("statistics").get("distribution")[2].get("count") if product.get("reviews") else 0,
                "rating_4": product.get("reviews").get("statistics").get("distribution")[1].get("count") if product.get("reviews") else 0,
                "rating_5": product.get("reviews").get("statistics").get("distribution")[0].get("count") if product.get("reviews") else 0,
            })

            brand_info.append({
                "id": brand_id,
                "name": product.get("brand").get("name"),
            })

        df_product = pd.DataFrame(product_info)
        df_brand = pd.DataFrame(brand_info)

        lock.acquire()
        if not os.path.exists(defs.PRODUCT_FILE_DUPLICATES_CSV):
            df_product.to_csv(defs.PRODUCT_FILE_DUPLICATES_CSV, index=False)
        else:
            df_product.to_csv(defs.PRODUCT_FILE_DUPLICATES_CSV, mode="a", index=False, header=False)

        if not os.path.exists(defs.BRAND_FILE_DUPLICATES_CSV):
            df_brand.to_csv(defs.BRAND_FILE_DUPLICATES_CSV, index=False)
        else:
            df_brand.to_csv(defs.BRAND_FILE_DUPLICATES_CSV, mode="a", index=False, header=False)
        lock.release()

        pages_left = products_json["data"]["pagination"]["total_pages"] - working_page
        if pages_left == 0:
            break

        working_page += 1
