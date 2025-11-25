import pymysql
from datetime import datetime
import traceback
import test_connection as cn

# ==============================
# KẾT NỐI DATABASE
# ==============================
def get_staging_connection():
    return cn.get_staging_connection()
def get_connection():
    conn = cn.get_control_connection()
    if not conn:
        print("❌ Không thể kết nối tới database CONTROL.")
    return conn
# ==============================
# HÀM LÀM SẠCH DỮ LIỆU
# ==============================
import re

def clean_price(price):
    if not price:
        return None
    # Xóa các ký tự không phải số
    price = str(price)
    price = price.replace(".", "").replace(",", "").replace("₫", "").replace("đ", "").strip()
    price = re.sub(r"[^0-9]", "", price)
    if price == "":
        return 0
    return float(price)


def clean_discount(value):
    """'13%' → 13"""
    try:
        if not value:
            return 0
        v = str(value).replace("%", "").replace(",", "").strip()
        return int(float(v)) if v else 0
    except:
        return 0

def parse_datetime_safe(val):
    """Chuyển đổi chuỗi datetime an toàn"""
    if not val:
        return datetime.now()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(val), fmt)
        except:
            continue
    return datetime.now()

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
# LẤY CẤU HÌNH JOB TỪ BẢNG CONFIG
# ==============================
def get_config(job_name):
    conn = cn.get_control_connection()
    if not conn:
        return None
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("SELECT * FROM config WHERE job_name = %s AND active = 1", (job_name,))
        conf = cursor.fetchone()
    conn.close()
    return conf

# ==============================
# CHẠY TRANSFORM CLEAN
# ==============================
def transform_clean():
    conn = get_staging_connection()
    if not conn:
        print("❌ Không thể kết nối tới database staging.")
        return

    try:
        src = conn.cursor(pymysql.cursors.DictCursor)
        dst = conn.cursor()

        # Lấy dữ liệu thô
        src.execute("SELECT * FROM stg_cellphones;")
        rows = src.fetchall()

        if not rows:
            print("⚠️ Không có dữ liệu trong stg_cellphones.")
            return

        print(f"📦 Đang xử lý {len(rows)} dòng...")

        # Xóa dữ liệu cũ trong bảng clean
        dst.execute("TRUNCATE TABLE stg_cellphones_cleans;")

        count = 0
        for r in rows:
            pname = (r.get("product_name") or "").strip()
            if not pname:
                continue
            price_clean = clean_price(r.get("price"))
            old_price_clean = clean_price(r.get("old_price"))
            discount_clean = clean_discount(r.get("discount_percent"))

           

            dst.execute("""
                INSERT INTO stg_cellphones_cleans
                (product_name, brand, price, old_price, discount_percent,
                 additional_info, image_url, crawl_date, crawl_time, full_date,
                 product_url, source_name, source_url)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                pname,
                r.get("brand_name") or "",
                clean_price(r.get("price")),
                clean_price(r.get("old_price")),
                clean_discount(r.get("discount_percent")),
                r.get("additional_info") or "",
                r.get("image_url") or "",
                r.get("crawl_date"),
                r.get("crawl_time"),
                parse_datetime_safe(r.get("full_date")),
                r.get("product_url") or "",
                r.get("source_name") or "",
                r.get("source_url") or ""
            ))
            
            count += 1
            


        conn.commit()
        print(f"✅ Đã làm sạch và lưu {count} dòng vào stg_cellphones_cleans.")
    
    except Exception as e:
        print("❌ Lỗi khi transform clean:", e)
        traceback.print_exc()

    finally:
        conn.close()
def update_scd2():
    conn = get_staging_connection()
    if not conn:
        print("❌ Không thể kết nối tới database staging.")
        return

    try:
        src = conn.cursor(pymysql.cursors.DictCursor)
        dst = conn.cursor(pymysql.cursors.DictCursor)

        # 1️⃣ Lấy dữ liệu sạch từ bảng cleans
        src.execute("SELECT * FROM stg_cellphones_cleans;")
        new_rows = src.fetchall()

        if not new_rows:
            print("⚠️ Không có dữ liệu mới để cập nhật SCD2.")
            return

        print(f"📦 Đang xử lý {len(new_rows)} dòng để cập nhật SCD2...")

        for r in new_rows:
            natural_key = r.get("product_url") or r.get("product_name")

            # 2️⃣ Kiểm tra bản ghi hiện tại trong SCD2
            dst.execute("""
                SELECT * FROM stg_cellphones_scd2 
                WHERE natural_key = %s AND is_current = 1
            """, (natural_key,))
            current = dst.fetchone()

            # 3️⃣ Nếu chưa có -> thêm mới
            if not current:
                dst.execute("""
                    INSERT INTO stg_cellphones_scd2
                    (natural_key, product_name, brand, current_price, original_price, discount_percent,
                     additional_info, image_url, crawl_date, crawl_time, full_date, product_url, 
                     source_name, source_url, valid_from, valid_to, is_current)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NULL,1)
                """, (
                    natural_key,
                    r.get("product_name"),
                    r.get("brand"),
                    r.get("price"),
                    r.get("old_price"),
                    r.get("discount_percent"),
                    r.get("additional_info"),
                    r.get("image_url"),
                    r.get("crawl_date"),
                    r.get("crawl_time"),
                    r.get("full_date"),
                    r.get("product_url"),
                    r.get("source_name"),
                    r.get("source_url")
                ))
                continue

            # 4️⃣ Nếu có -> so sánh thay đổi
            changed = (
                float(current.get("current_price") or 0) != float(r.get("price") or 0)
                or float(current.get("original_price") or 0) != float(r.get("old_price") or 0)
                or int(current.get("discount_percent") or 0) != int(r.get("discount_percent") or 0)
                or (current.get("additional_info") or "").strip() != (r.get("additional_info") or "").strip()
            )

            if changed:
                # 5️⃣ Cập nhật bản ghi cũ thành không còn hiệu lực
                dst.execute("""
                    UPDATE stg_cellphones_scd2
                    SET is_current = 0, valid_to = NOW()
                    WHERE natural_key = %s AND is_current = 1
                """, (natural_key,))

                # 6️⃣ Thêm bản ghi mới với giá trị cập nhật
                dst.execute("""
                    INSERT INTO stg_cellphones_scd2
                    (natural_key, product_name, brand, current_price, original_price, discount_percent,
                     additional_info, image_url, crawl_date, crawl_time, full_date, product_url,
                     source_name, source_url, valid_from, valid_to, is_current)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NULL,1)
                """, (
                    natural_key,
                    r.get("product_name"),
                    r.get("brand"),
                    r.get("price"),
                    r.get("old_price"),
                    r.get("discount_percent"),
                    r.get("additional_info"),
                    r.get("image_url"),
                    r.get("crawl_date"),
                    r.get("crawl_time"),
                    r.get("full_date"),
                    r.get("product_url"),
                    r.get("source_name"),
                    r.get("source_url")
                ))

        conn.commit()
        print("✅ Đã cập nhật dữ liệu vào stg_cellphones_scd2 theo SCD Type 2.")

    except Exception as e:
        print("❌ Lỗi khi cập nhật SCD2:", e)
        traceback.print_exc()
        conn.rollback()

    finally:
        conn.close()


# ==============================
# CHẠY JOB TỔNG HỢP
# ==============================
def run_job_transform(job_name="TRANSFORM_STG_CELLPHONES"):
    print(f"\n🚀 Bắt đầu job: {job_name} lúc {datetime.now().strftime('%H:%M:%S')}")
    control_conn = cn.get_control_connection()
    log_id = log_job_start(control_conn, job_name) if control_conn else None

    try:
        conf = get_config(job_name)
        if not conf:
            msg = f"⚠️ Không tìm thấy config hoặc job {job_name} đang bị inactive."
            print(msg)
            if control_conn and log_id:
                log_job_end(control_conn, log_id, "DISABLED", msg)
                update_job_status(control_conn, job_name, "DISABLED")
            return

        # THỰC THI CÁC BƯỚC
        print("🔹 Bắt đầu bước transform_clean()...")
        transform_clean()

        print("🔹 Bắt đầu bước update_scd2()...")
        update_scd2()

        msg = "✅ Hoàn tất job transform + SCD2 thành công."
        print(msg)
        if control_conn and log_id:
            log_job_end(control_conn, log_id, "SUCCESS", msg)
            update_job_status(control_conn, job_name, "SUCCESS")

    except Exception as e:
        error_message = traceback.format_exc()
        print(f"❌ Lỗi trong job {job_name}:", error_message)
        if control_conn and log_id:
            log_job_end(control_conn, log_id, "FAILED", error_message)
            update_job_status(control_conn, job_name, "FAILED")

    finally:
        if control_conn:
            control_conn.close()


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    run_job_transform()


