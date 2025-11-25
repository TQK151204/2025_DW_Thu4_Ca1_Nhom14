import requests # để gửi HTTP request
from bs4 import BeautifulSoup # để parser html
from datetime import datetime
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed # để chạy đa luồng
# Đặt User-Agent giả lập trình duyệt để tránh website chặn request tự động.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

CATEGORIES = [
    "https://cellphones.com.vn/mobile/apple/iphone-11-series.html",
    "https://cellphones.com.vn/mobile/apple/iphone-12-vna.html",
    "https://cellphones.com.vn/mobile/apple/iphone-13.html",
    "https://cellphones.com.vn/mobile/apple/iphone-14.html",
    "https://cellphones.com.vn/mobile/apple/iphone-15.html",
    "https://cellphones.com.vn/mobile/apple/iphone-16.html",
    "https://cellphones.com.vn/mobile/apple/iphone-air.html",
    "https://cellphones.com.vn/mobile/apple/iphone-17.html"
]

OUTPUT_DIR = "StagingArea/crawl_data/cellphones"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# TẢI HTML Gửi request HTTP GET tới URL. Nếu có lỗi (404, timeout, server error) → in lỗi và trả về None.
def get_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f" Lỗi tải trang {url}: {e}")
        return None



def parse_item(item, crawl_datetime):
    source_name = "CellphoneS"
    try:
        name_tag = item.select_one("h3") or item.select_one(".product__title")
        name = name_tag.text.strip() if name_tag else "Không rõ tên"

        price_now_tag = item.select_one("p.product__price--show")
        price_now = price_now_tag.text.strip().replace("₫", "").replace(".", "") if price_now_tag else "0"

        price_old_tag = item.select_one("p.product__price--through")
        price_old = price_old_tag.text.strip().replace("₫", "").replace(".", "") if price_old_tag else "0"

        discount_tag = item.select_one("div.product__price--percent span")
        discount = discount_tag.text.strip() if discount_tag else "0%"

        img_tag = item.select_one("img")
        image = img_tag.get("data-src") or img_tag.get("src") if img_tag else ""

        link_tag = item.select_one("a")
        if link_tag:
            product_url = link_tag.get("href", "")
            if not product_url.startswith("http"):
                product_url = "https://cellphones.com.vn" + product_url
        else:
            product_url = ""

        return {
            "product_name": name,
            "brand_name": "Apple",
            "price": price_now,
            "old_price": price_old,
            "discount_percent": discount,
            "image_url": image,
            "product_url": product_url,
            "source_name": source_name,
            "source_url": "https://cellphones.com.vn",
            "crawl_date": crawl_datetime.strftime("%Y-%m-%d"),
            "crawl_time": crawl_datetime.strftime("%H:%M:%S"),
        }
    except:
        return None

# # Gọi get_html(url) → trả về HTML.
# Parse HTML để tìm tất cả thẻ chứa sản phẩm.
# Duyệt từng sản phẩm → gọi parse_item() → lưu vào list.
# Nếu lỗi tải HTML → trả về danh sách rỗng.
def crawl_category(url):
    print(f" Crawl danh mục: {url}")
    html = get_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("div.product-info-container.product-item, div.product__item")

    print(f" Tìm thấy {len(items)} sản phẩm")

    crawl_datetime = datetime.now()
    iphones = []

    for item in items:
        product = parse_item(item, crawl_datetime)
        if product:
            iphones.append(product)

    return iphones



# Dùng ThreadPoolExecutor: crawl nhiều URL đồng thời, nhanh hơn tuần tự.
# Duyệt as_completed(tasks) → khi một task xong thì thêm dữ liệu vào all_products.
# Trả về list tất cả sản phẩm.
# Crawl tất cả danh mục (multi-thread)
def crawl_celphones(url_list):
    all_products = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        tasks = [executor.submit(crawl_category, url) for url in url_list]
        for task in as_completed(tasks):
            all_products.extend(task.result())

    return all_products



# Lưu CSV
def save_to_csv_cellphones(data, output_dir):
    if not data:
        print(" Không có dữ liệu để lưu CSV")
        return None

    os.makedirs(output_dir, exist_ok=True)
    filename = f"Cellphones_Product_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    filepath = os.path.join(output_dir, filename)
    # Dùng DictWriter → ghi header + rows.
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    print(f" Lưu file CSV thành công: {filepath}")
    return filepath   # trả về đường dẫn file



# Test trực tiếp
if __name__ == "__main__":
    products = crawl_celphones(url_list=CATEGORIES)
    print(f" Tổng số sản phẩm: {len(products)}")
    save_to_csv_cellphones(products, "StagingArea/crawl_data/cellphones")