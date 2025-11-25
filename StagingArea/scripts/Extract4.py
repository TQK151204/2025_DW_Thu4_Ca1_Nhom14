import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# =========================
# ⚡ Cấu hình
# =========================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

CATEGORIES = [
    "https://hoanghamobile.com/dien-thoai-di-dong/iphone/iphone-13-series",
    "https://hoanghamobile.com/dien-thoai-di-dong/iphone/iphone-14-series",
    "https://hoanghamobile.com/dien-thoai-di-dong/iphone/iphone-15-series",
    "https://hoanghamobile.com/dien-thoai-di-dong/iphone/iphone-16-series",
    "https://hoanghamobile.com/dien-thoai-di-dong/iphone/iphone-17-series"
]

OUTPUT_DIR = "StagingArea/crawl_data/hoanghamobile"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =========================
# 🔗 Tải HTML với retry và delay
# =========================
def get_html(url, retries=4, timeout=25):
    for attempt in range(1, retries + 1):
        try:
            print(f"🔗 Đang tải: {url} (lần {attempt})")
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            
            # Nghỉ ngẫu nhiên 1–2 giây để tránh bị chặn
            time.sleep(random.uniform(1.0, 2.0))
            return response.text

        except Exception as e:
            print(f"⚠️ Lỗi tải trang {url} (lần {attempt}): {e}")
            if attempt < retries:
                time.sleep(2)
            else:
                return None


# =========================
# 📦 Parse 1 sản phẩm
# =========================
def parse_item(item, crawl_datetime):
    """Trích xuất thông tin sản phẩm theo chuẩn"""
    try:
        source_name = "Hoanghamobile"

        # Tên sản phẩm
        name_tag = item.select_one("h3.product-name") or item.select_one("h3 a")
        name = name_tag.text.strip() if name_tag else "Không rõ tên"

        # Giá cũ
        price_old_tag = item.select_one("div.price.price-last strike")
        price_old = price_old_tag.text.strip().replace("₫", "").replace(".", "") if price_old_tag else "0"

        # Giảm giá
        discount_tag = item.select_one("div.price.price-last span")
        discount = discount_tag.text.strip().replace("-", "") if discount_tag else "0"

        # Giá hiện tại
        price_now_tag = item.select_one("div.price strong")
        price_now = price_now_tag.text.strip().replace("₫", "").replace(".", "") if price_now_tag else "0"

        # Ảnh sản phẩm (lọc ảnh chính)
        image_url = ""
        for img_tag in item.select("img"):
            img_src = img_tag.get("src") or img_tag.get("data-src") or ""
            if img_src.startswith("/"):
                img_src = "https://hoanghamobile.com" + img_src
            if "/Uploads/" in img_src and "sticker" not in img_src.lower() and "icon" not in img_src.lower():
                image_url = img_src
                break

        # Link sản phẩm
        link_tag = item.select_one("a")
        product_url = link_tag.get("href", "") if link_tag else ""
        if product_url and not product_url.startswith("http"):
            product_url = "https://hoanghamobile.com" + product_url

        return {
            "product_name": name,
            "brand_name": "Apple",
            "price": price_now,
            "old_price": price_old,
            "discount_percent": discount,
            "image_url": image_url,
            "product_url": product_url,
            "source_name": source_name,
            "source_url": "https://hoanghamobile.com",
            "crawl_date": crawl_datetime.strftime("%Y-%m-%d"),
            "crawl_time": crawl_datetime.strftime("%H:%M:%S"),
        }
    except Exception as e:
        print(f"⚠️ Lỗi parse sản phẩm: {e}")
        return None


# =========================
# 🛒 Crawl 1 danh mục
# =========================
def crawl_category(url):
    print(f"⏳ Crawl danh mục: {url}")
    html = get_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("div.pj16-item")
    print(f"🔍 Tìm thấy {len(items)} sản phẩm")

    crawl_datetime = datetime.now()
    products = [parse_item(item, crawl_datetime) for item in items if parse_item(item, crawl_datetime)]
    return products


# =========================
# 🏃 Crawl nhiều danh mục (multi-thread)
# =========================
def crawl_hoanghamobile(url_list):
    all_products = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        tasks = [executor.submit(crawl_category, url) for url in url_list]
        for task in as_completed(tasks):
            all_products.extend(task.result())

    return all_products


# =========================
# 💾 Lưu dữ liệu CSV
# =========================
def save_to_csv_hoanghamobile(data, output_dir):
    if not data:
        print("⚠️ Không có dữ liệu để lưu CSV")
        return None

    os.makedirs(output_dir, exist_ok=True)
    filename = f"HoangHaMobile_Product_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    print(f"💾 Lưu file CSV thành công: {filepath}")
    return filepath


# =========================
# 🔹 Test trực tiếp
# =========================
if __name__ == "__main__":
    products = crawl_hoanghamobile(CATEGORIES)
    print(f"✅ Tổng số sản phẩm: {len(products)}")
    save_to_csv_hoanghamobile(products, OUTPUT_DIR)
