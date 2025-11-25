import pymysql
import csv
from datetime import datetime
import test_connection as cn
import os

def load_dim_date(csv_path: str):
    """
    Load dữ liệu dim_date từ CSV vào database DW một lần.
    
    Args:
        csv_path (str): Đường dẫn tới file CSV dim_date.
    """
    TABLE_NAME = "dim_date"
    
    # 1️⃣ Kiểm tra file CSV tồn tại
    if not os.path.exists(csv_path):
        print(f"❌ Không tìm thấy file CSV: {csv_path}")
        return
    
    # 2️⃣ Kết nối DW
    conn = cn.get_dw_connection()
    if not conn:
        print("❌ Không thể kết nối tới database DW.")
        return
    cursor = conn.cursor()
    
    try:
        # 3️⃣ Tạo bảng nếu chưa tồn tại
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date_sk INT PRIMARY KEY,
            full_date DATE NOT NULL,
            day_since_2025 INT,
            month_since_2025 INT,
            day_of_week VARCHAR(10),
            calendar_month VARCHAR(15),
            calendar_year INT,
            calendar_year_month VARCHAR(10),
            day_of_month INT,
            day_of_year INT,
            week_of_year_sunday INT,
            year_week_sunday VARCHAR(10),
            week_sunday_start DATE,
            week_of_year_monday INT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"✅ Bảng {TABLE_NAME} đã tạo xong (nếu chưa có)")
        
        # 4️⃣ Load CSV
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # 5️⃣ Convert kiểu dữ liệu
        for r in rows:
            r['date_sk'] = int(r['date_sk'])
            r['day_since_2025'] = int(r['day_since_2025'])
            r['month_since_2025'] = int(r['month_since_2025'])
            r['calendar_year'] = int(r['calendar_year'])
            r['day_of_month'] = int(r['day_of_month'])
            r['day_of_year'] = int(r['day_of_year'])
            r['week_of_year_sunday'] = int(r['week_of_year_sunday'])
            r['week_of_year_monday'] = int(r['week_of_year_monday'])
            r['full_date'] = datetime.strptime(r['full_date'], "%Y-%m-%d").date()
            r['week_sunday_start'] = datetime.strptime(r['week_sunday_start'], "%Y-%m-%d").date()
        
        # 6️⃣ Xóa dữ liệu cũ
        
        
        # 7️⃣ Insert dữ liệu
        insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            date_sk, full_date, day_since_2025, month_since_2025,
            day_of_week, calendar_month, calendar_year, calendar_year_month,
            day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
            week_sunday_start, week_of_year_monday
        ) VALUES (
            %(date_sk)s, %(full_date)s, %(day_since_2025)s, %(month_since_2025)s,
            %(day_of_week)s, %(calendar_month)s, %(calendar_year)s, %(calendar_year_month)s,
            %(day_of_month)s, %(day_of_year)s, %(week_of_year_sunday)s, %(year_week_sunday)s,
            %(week_sunday_start)s, %(week_of_year_monday)s
        )
        """
        cursor.executemany(insert_sql, rows)
        conn.commit()
        print(f"✅ Dữ liệu đã load vào bảng {TABLE_NAME} thành công ({len(rows)} rows)")
    
    except Exception as e:
        print(f"❌ Lỗi khi load dim_date: {e}")
    
    finally:
        # 8️⃣ Đóng kết nối
        cursor.close()
        conn.close()


# ==============================
# Ví dụ gọi hàm
# ==============================
if __name__ == "__main__":
    base_path = r"C:\Users\Tran nam anh\Python\2025_DW_Thu4_Ca1_Nhom14\StagingArea\scripts"
    CSV_FILE = os.path.join(base_path, "dim_date.csv")
    load_dim_date(CSV_FILE)
