import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================
# 🛡 Cấu hình headers
# ============================================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# ============================================
# 🌐 Danh sách URL các series iPhone trên TGDD
# ============================================
CATEGORIES = [
    "https://www.thegioididong.com/dtdd-apple-iphone-13-series",
    "https://www.thegioididong.com/dtdd-apple-iphone-14-series",
    "https://www.thegioididong.com/dtdd-apple-iphone-15-series",
    "https://www.thegioididong.com/dtdd-apple-iphone-16-series",
    "https://www.thegioididong.com/dtdd-apple-iphone-air",
    "https://www.thegioididong.com/dtdd-apple-iphone-17-series"
]

# Thư mục lưu CSV
OUTPUT_DIR = "StagingArea/crawl_data/tgdd"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================
# 🔹 Tải HTML từ URL
# ============================================
def get_html(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"⚠️ Lỗi tải trang {url}: {e}")
        return None


# ============================================
# 🔹 Parse 1 sản phẩm
# ============================================
def parse_item(item, crawl_datetime):
    """Trích xuất thông tin sản phẩm theo định dạng chuẩn"""
    try:
        # Tên sản phẩm
        name_tag = item.select_one("h3.box-title") or item.select_one("h3")
        name = name_tag.text.strip() if name_tag else "Không rõ tên"

        # Giá hiện tại
        price_now_tag = item.select_one("strong.price")
        price_now = price_now_tag.text.strip().replace(".", "").replace("₫", "") if price_now_tag else "0"

        # Giá cũ
        price_old_tag = item.select_one("p.price-old.black")
        price_old = price_old_tag.text.strip().replace(".", "").replace("₫", "") if price_old_tag else "0"

        # Giảm giá
        discount_tag = item.select_one("span.percent")
        discount = discount_tag.text.strip().replace("-", "") if discount_tag else "0"

        # Ảnh
        img_tag = item.select_one("img")
        image_url = img_tag.get("src") or img_tag.get("data-src") if img_tag else ""

        # Link sản phẩm
        link_tag = item.select_one("a")
        product_url = link_tag.get("href", "") if link_tag else ""
        if product_url and not product_url.startswith("http"):
            product_url = "https://www.thegioididong.com" + product_url

        return {
            "product_name": name,
            "brand_name": "Apple",
            "price": price_now,
            "old_price": price_old,
            "discount_percent": discount,
            "image_url": image_url,
            "product_url": product_url,
            "source_name": "Thegioididong",
            "source_url": "https://www.thegioididong.com",
            "crawl_date": crawl_datetime.strftime("%Y-%m-%d"),
            "crawl_time": crawl_datetime.strftime("%H:%M:%S"),
        }
    except Exception as e:
        print(f"⚠️ Lỗi parse sản phẩm: {e}")
        return None


# ============================================
# 🔹 Crawl 1 danh mục
# ============================================
def crawl_category(url):
    print(f"⏳ Crawl danh mục: {url}")
    html = get_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("ul.listproduct li")
    print(f"🔍 Tìm thấy {len(items)} sản phẩm")

    crawl_datetime = datetime.now()
    products = [parse_item(item, crawl_datetime) for item in items if parse_item(item, crawl_datetime)]
    return products


# ============================================
# 🔹 Crawl nhiều danh mục (multi-thread)
# ============================================
def crawl_tgdd(url_list):
    all_products = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        tasks = [executor.submit(crawl_category, url) for url in url_list]
        for task in as_completed(tasks):
            all_products.extend(task.result())

    return all_products


# ============================================
# 🔹 Lưu dữ liệu ra CSV
# ============================================
def save_to_csv_tgdd(data, output_dir):
    if not data:
        print("⚠️ Không có dữ liệu để lưu CSV")
        return None

    os.makedirs(output_dir, exist_ok=True)
    filename = f"TGDD_Product_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    filepath = os.path.normpath(os.path.join(output_dir, filename))

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    print(f"💾 Lưu file CSV thành công: {filepath.replace(os.sep, '/')}")
    return filepath


# ============================================
# 🔹 TEST TRỰC TIẾP
# ============================================
if __name__ == "__main__":
    products = crawl_tgdd(CATEGORIES)
    print(f"✅ Tổng số sản phẩm: {len(products)}")
    save_to_csv_tgdd(products, OUTPUT_DIR)
