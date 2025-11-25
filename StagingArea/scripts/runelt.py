import os
from datetime import datetime, timedelta
import traceback

# ✅ Import các module ETL
from load_to_staging import run_job_load_staging_input_days_ago
from transform import run_job_transform
from load_dw import run_job_load_dw


# ==============================
# 🧩 HÀM CHẠY FULL ELT CHO NGÀY NHẬP
# ==============================
def run_full_elt_for_date(date_str: str):
    """
    Chạy full ELT (Load STAGING → Transform → Load DW) cho một ngày bất kỳ.

    Args:
        date_str (str): Ngày cần chạy, định dạng 'YYYY-MM-DD'
    """
    print(f"\n🚀 Bắt đầu ELT cho ngày {date_str}...\n")

    try:
        # --- 1️⃣ Kiểm tra thư mục crawl ---
        crawl_folder = "StagingArea/crawl_data/cellphones"
        if not os.path.exists(crawl_folder):
            print(f"⚠️ Thư mục crawl {crawl_folder} không tồn tại.")
            return

        # --- 2️⃣ Tính days_ago so với hôm nay ---
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            print("⚠️ Định dạng ngày không hợp lệ. Vui lòng nhập YYYY-MM-DD.")
            return

        today = datetime.today().date()
        days_ago = (today - target_date).days
        if days_ago < 0:
            print(f"⚠️ Ngày {date_str} chưa tới, không thể chạy ELT.")
            return

        print(f"📅 Load dữ liệu cách {days_ago} ngày so với hôm nay")

        # --- 3️⃣ Load vào STAGING ---
        print("🔹 Load vào STAGING...")
        csv_path = run_job_load_staging_input_days_ago(
            job_name="LOAD_STG_CELLPHONES",
            days_ago=days_ago
        )

        if not csv_path:
            print("⚠️ Load STAGING thất bại, kết thúc ELT.")
            return
        print(f"✅ Load STAGING thành công file: {csv_path}")

        # --- 4️⃣ Transform dữ liệu ---
        print("🔹 Transform dữ liệu...")
        run_job_transform(job_name="TRANSFORM_STG_CELLPHONES")
        print("✅ Transform hoàn tất.")

        # --- 5️⃣ Load vào Data Warehouse ---
        print("🔹 Load vào Data Warehouse...")
        run_job_load_dw(job_name="LOAD_TO_DW_CELLPHONES")
        print("✅ Load DW hoàn tất.")

        print(f"🎉 ELT cho ngày {date_str} hoàn tất thành công!\n")

    except Exception as e:
        print(f"❌ Lỗi khi chạy ELT cho ngày {date_str}:\n{traceback.format_exc()}")


# ==============================
# 🏁 MAIN
# ==============================
if __name__ == "__main__":
    date_input = input("📅 Nhập ngày cần chạy ELT (YYYY-MM-DD): ").strip()
    run_full_elt_for_date(date_input)
