import requests
from bs4 import BeautifulSoup
import xlwings as xw
import os


def scrape_iphones(url="https://cellphones.com.vn/mobile/apple.html"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Accept-Language": "vi-VN,vi;q=0.9"
    }

    resp = requests.get(url, headers=headers)
    resp.encoding = resp.apparent_encoding

    if resp.status_code != 200:
        print("❌ Không thể truy cập trang, status code:", resp.status_code)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Mỗi sản phẩm nằm trong thẻ div có class sau:
    products = soup.select("div.product-info-container.product-item")

    iphones = []
    for item in products:
        # 🔹 Tên sản phẩm (đúng cấu trúc HTML bạn đưa)
        name_tag = item.select_one("div.product__name h3")
        name = name_tag.get_text(strip=True) if name_tag else "Không rõ tên"

        # 🔹 Giá hiện tại
        price_tag = item.select_one("p.product__price--show")
        price_now = price_tag.get_text(strip=True) if price_tag else "Chưa có giá"

        # 🔹 Giá gốc
        old_price_tag = item.select_one("p.product__price--through")
        old_price = old_price_tag.get_text(strip=True) if old_price_tag else "Không có giá gốc"

        # 🔹 Phần trăm giảm giá
        discount_percent_tag = item.select_one("div.product__price--percent span")
        discount_percent = discount_percent_tag.get_text(strip=True) if discount_percent_tag else "Không có"

        # 🔹 Thông tin phụ (màn hình, dung lượng,...)
        info_tags = item.select("div.product__badge p.product__more-info__item")
        info_list = [p.get_text(strip=True) for p in info_tags]
        info_text = " | ".join(info_list) if info_list else "Không có thông tin"

        
        # 🔹 Ảnh sản phẩm
        img_tag = item.select_one("img.product__img")
        image = img_tag["src"] if img_tag and img_tag.has_attr("src") else "Không có ảnh"

        iphones.append({
            "Tên": name,
            "Giá hiện tại": price_now,
            "Giá gốc": old_price,
            "Giảm (%)": discount_percent,
            "Thông tin phụ": info_text,
            "Ảnh": image
        })

    return iphones

# 🧾 Ghi dữ liệu ra Excel bằng xlwings
def save_to_excel(data, filename="danh_sach_iphone.xlsx"):
    if not data:
        print("⚠️ Không có dữ liệu để ghi.")
        return

    # 🗂️ Tạo thư mục 'data' nếu chưa có
    folder = "Data"
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    # Mở Excel (ẩn)
    app = xw.App(visible=False)
    wb = app.books.add()
    sheet = wb.sheets[0]
    sheet.name = "Danh sách iPhone"

    # Ghi tiêu đề
    headers = list(data[0].keys())
    sheet.range("A1").value = headers

    # Ghi nội dung
    for i, row in enumerate(data, start=2):
        sheet.range(f"A{i}").value = list(row.values())

    # Auto-fit cột
    sheet.autofit()

    # Lưu file
    wb.save(filepath)
    wb.close()
    app.quit()

    print(f"✅ Đã lưu dữ liệu vào file: {filepath}")


# 🚀 Chạy chương trình
if __name__ == "__main__":
    iphones = scrape_iphones()
    if iphones:
        save_to_excel(iphones)
    else:
        print("⚠️ Không lấy được dữ liệu (trang có thể render bằng JavaScript).")

# # 🧾 In ra kết quả
# if __name__ == "__main__":
#     danh_sach = scrape_iphones()
#     if not danh_sach:
#         print("⚠️ Không lấy được dữ liệu (có thể trang dùng JavaScript).")
#     else:
#         for i, iphone in enumerate(danh_sach, 1):
#             print(f"{i}. 📱 {iphone['Tên']}")
#             print(f"   💰 Giá hiện tại: {iphone['Giá hiện tại']}")
#             print(f"   💸 Giá gốc: {iphone['Giá gốc']}")
#             print(f"   🔻 Giảm: {iphone['Giảm (%)']}")
#             print(f"   📏 Thông tin: {iphone['Thông tin phụ']}")
#             print(f"   🖼️ Ảnh: {iphone['Ảnh']}")
#             print("-" * 80)