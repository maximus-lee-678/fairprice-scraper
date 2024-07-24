import download
import process
import defs
from pathlib import Path

def main():
    Path(defs.CATEGORY_FILE_FINAL_CSV).unlink(missing_ok=True)

    print("Polling category api.")
    category_response = download.get_category_json(write_to_file=False)
    category_info = process.get_all_categories(category_response)

    slug_list = [category["url"] for category in category_info]
    while None in slug_list:
        slug_list.remove(None)
    print(f"Found {len(slug_list)} urls from category api.")

    return slug_list


if __name__ == "__main__":
    main()
