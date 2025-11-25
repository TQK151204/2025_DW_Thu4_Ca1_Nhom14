import pymysql
from datetime import datetime
import traceback
import test_connection as cn

# ==============================
# 1️⃣ KẾT NỐI DATABASE
# ==============================
def get_staging_connection():
    return cn.get_staging_connection()

def get_dw_connection():
    return cn.get_dw_connection()

# ==============================
# 2️⃣ HÀM CHUẨN HÓA
# ==============================
def parse_date_safe(val):
    """Chuyển giá trị thành date chuẩn YYYY-MM-DD."""
    if not val:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(val), fmt).date()
        except Exception:
            continue
    return None


def parse_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0

# ==============================
# 📝 LOG JOB
# ==============================
def log_job_start(control_conn, job_name):
    with control_conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO job_log (job_name, start_time, status)
            VALUES (%s, NOW(), 'RUNNING')
        """, (job_name,))
        control_conn.commit()
        return cursor.lastrowid

def log_job_end(control_conn, job_id, status, message):
    with control_conn.cursor() as cursor:
        cursor.execute("""
            UPDATE job_log
            SET end_time = NOW(), status = %s, message = %s
            WHERE job_id = %s
        """, (status, message, job_id))
        control_conn.commit()

def update_job_status(control_conn, job_name, status):
    with control_conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO job_status (job_name, last_run, status)
            VALUES (%s, NOW(), %s)
            ON DUPLICATE KEY UPDATE last_run = NOW(), status = %s
        """, (job_name, status, status))
        control_conn.commit()

# ==============================
# 📌 LẤY CẤU HÌNH JOB TỪ CONTROL DB
# ==============================
def get_config(job_name):
    conn = cn.get_control_connection()
    result = None
    if conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM config WHERE job_name=%s AND active=1", (job_name,))
            result = cursor.fetchone()
        conn.close()
    return result
# ==============================
# 3️⃣ LOAD STAGING → DIM + FACT
# ==============================
def load_staging_to_dw(table_staging="stg_cellphones_scd2"):
    src_conn = get_staging_connection()
    dw_conn = get_dw_connection()
    if not src_conn or not dw_conn:
        print("❌ Không thể kết nối đến database.")
        return

    try:
        src_cursor = src_conn.cursor(pymysql.cursors.DictCursor)
        dw_cursor = dw_conn.cursor(pymysql.cursors.DictCursor)

        src_cursor.execute(f"SELECT * FROM {table_staging};")
        rows = src_cursor.fetchall()
        if not rows:
            print(f"⚠️ Không có dữ liệu trong {table_staging}")
            return

        print(f"📦 Đang xử lý {len(rows)} dòng từ {table_staging}")
        count = 0

        for row in rows:
            # --- 1️⃣ Dim Brand ---
            brand_name = (row.get("brand") or "").strip() or "Unknown"
            dw_cursor.execute("SELECT brand_id FROM dim_brand WHERE brand_name=%s", (brand_name,))
            brand = dw_cursor.fetchone()
            if brand:
                brand_id = brand["brand_id"]
            else:
                dw_cursor.execute("INSERT INTO dim_brand (brand_name) VALUES (%s)", (brand_name,))
                dw_conn.commit()
                brand_id = dw_cursor.lastrowid

            # --- 2️⃣ Dim Product ---
            product_name = (row.get("product_name") or "").strip()
            if not product_name:
                continue  # bỏ dòng rỗng
            dw_cursor.execute("SELECT product_id FROM dim_product WHERE product_name=%s", (product_name,))
            product = dw_cursor.fetchone()
            if product:
                product_id = product["product_id"]
            else:
                dw_cursor.execute("""
                    INSERT INTO dim_product (product_name, additional_info, image_url, product_url, brand_id)
                    VALUES (%s,%s,%s,%s,%s)
                """, (
                    product_name,
                    row.get("additional_info") or "",
                    row.get("image_url") or "",
                    row.get("product_url") or "",
                    brand_id
                ))
                dw_conn.commit()
                product_id = dw_cursor.lastrowid

            # --- 3️⃣ Dim Source ---
            source_name = (row.get("source_name") or "").strip() or "Unknown"
            source_url = row.get("source_url") or ""
            dw_cursor.execute("SELECT source_id FROM dim_source WHERE source_name=%s", (source_name,))
            source = dw_cursor.fetchone()
            if source:
                source_id = source["source_id"]
            else:
                dw_cursor.execute("INSERT INTO dim_source (source_name, source_url) VALUES (%s,%s)",
                                  (source_name, source_url))
                dw_conn.commit()
                source_id = dw_cursor.lastrowid

            # --- 4️⃣ Dim Date ---
            crawl_date = parse_date_safe(row.get("full_date"))
            if not crawl_date:
                print(f"⚠️ Không parse được ngày {row.get('full_date')} — bỏ qua dòng này.")
                continue

            # Lookup date_sk
            dw_cursor.execute("SELECT date_sk FROM dim_date WHERE full_date = %s", (crawl_date,))
            date_row = dw_cursor.fetchone()
            if not date_row:
                print(f"⚠️ Không tìm thấy ngày {crawl_date} trong dim_date — bỏ qua dòng này.")
                continue
            date_sk = date_row["date_sk"]



            # --- 5️⃣ Fact Table ---
            dw_cursor.execute("""
                INSERT INTO fact_product_pricing
                (product_id, brand_id, source_id, date_sk, price, discount_percent, crawl_date, crawl_time, full_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                product_id,
                brand_id,
                source_id,
                date_sk,
                parse_float(row.get("current_price")),
                parse_float(row.get("discount_percent")),
                row.get("crawl_date"),
                row.get("crawl_time"),
                crawl_date
            ))
            count += 1
            
        dw_conn.commit()
        print(f"🎯 Đã load thành công {count}/{len(rows)} dòng vào DW.")

    except Exception as e:
        print("❌ Lỗi khi load DW:")
        traceback.print_exc()

    finally:
        src_conn.close()
        dw_conn.close()

def run_job_load_dw(job_name="LOAD_TO_DW_CELLPHONES"):
    print(f"🚀 Bắt đầu job: {job_name}")
    
    control_conn = cn.get_control_connection()  # ✅ Mở kết nối DB control
    job_id = log_job_start(control_conn, job_name)  # ✅ Truyền connection + job_name
    rows_affected = 0

    try:
        conf = get_config(job_name)
        if not conf:
            msg = f"Không tìm thấy cấu hình cho job {job_name}"
            print("⚠️", msg)
            log_job_end(control_conn, job_id, "FAILED", msg)
            return

        source = conf["source"]
        target = conf["target"]
        print(f"📂 Nguồn: {source} → 🎯 Đích: {target}")

        # Gọi hàm load chính
        rows_affected = load_staging_to_dw(table_staging=source)
        log_job_end(control_conn, job_id, "SUCCESS", "Job completed successfully")
        update_job_status(control_conn, job_name, "SUCCESS")

        print(f"✅ Hoàn thành job: {job_name}")

    except Exception as e:
        msg = f"Lỗi khi chạy job {job_name}: {str(e)}"
        print("❌", msg)
        traceback.print_exc()
        log_job_end(control_conn, job_id, "FAILED", msg)
        update_job_status(control_conn, job_name, "FAILED")

    finally:
        control_conn.close()

# ==============================
# 4️⃣ CHẠY SCRIPT
# ==============================
if __name__ == "__main__":
    run_job_load_dw()
