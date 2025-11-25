import csv
import pymysql
from datetime import datetime,timedelta
import test_connection as cn  
import os
import traceback

# ==============================
# 🔌 KẾT NỐI DATABASE
# ==============================
def get_staging_connection():
    """Kết nối tới STAGING DB"""
    conn = cn.get_staging_connection()
    if not conn:
        print("❌ Không thể kết nối tới database STAGING.")
    return conn

def get_control_connection():
    """Kết nối tới CONTROL DB"""
    conn = cn.get_control_connection()
    if not conn:
        print("❌ Không thể kết nối tới database CONTROL.")
    return conn

# ==============================
# 📝 LOG JOB
# ==============================
def log_job_start(control_conn, job_name):
    """Ghi log bắt đầu job vào bảng job_log"""
    with control_conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO job_log (job_name, start_time, status)
            VALUES (%s, NOW(), 'RUNNING')
        """, (job_name,))
        control_conn.commit()
        return cursor.lastrowid  # Trả về job_id để update log sau

def log_job_end(control_conn, job_id, status, message):
    """Cập nhật log khi job kết thúc"""
    with control_conn.cursor() as cursor:
        cursor.execute("""
            UPDATE job_log
            SET end_time = NOW(), status = %s, message = %s
            WHERE job_id = %s
        """, (status, message, job_id))
        control_conn.commit()

def update_job_status(control_conn, job_name, status):
    """Cập nhật trạng thái job hiện tại vào bảng job_status"""
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
    """Lấy cấu hình job từ bảng config, chỉ lấy job active=1"""
    conn = get_control_connection()
    result = None
    if conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM config WHERE job_name=%s AND active=1", (job_name,))
            result = cursor.fetchone()
        conn.close()
    return result

# ==============================
# 🚀 LOAD CSV VÀO STAGING
# ==============================
def load_to_staging(csv_file_path, target_table):
    """
    Load dữ liệu từ file CSV vào bảng STAGING.
    - Truncate bảng trước khi insert
    - Bulk insert dữ liệu
    """
    rows_inserted = 0
    staging_conn = get_staging_connection()
    if not staging_conn:
        print("❌ Không thể kết nối STAGING DB.")
        return 0

    try:
        # Đọc CSV
        with open(csv_file_path, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            data = list(reader)

        if not data:
            print("⚠️ CSV trống, không có dữ liệu.")
            return 0

        # Làm sạch bảng STAGING
        with staging_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {target_table};")
            staging_conn.commit()
        print(f"🧹 Đã làm sạch bảng {target_table} trước khi load dữ liệu mới.")

        # Chuẩn bị dữ liệu insert (toàn bộ dạng text)
        insert_rows = []
        for row in data:
            insert_rows.append((
                str(row.get("product_name","")),
                str(row.get("brand_name","")),
                str(row.get("price","")),
                str(row.get("old_price","")),
                str(row.get("discount_percent","")),
                str(row.get("additional_info","")),
                str(row.get("image_url","")),
                str(row.get("product_url","")),
                str(row.get("source_name","")),
                str(row.get("source_url","")),
                str(row.get("crawl_date","")),
                str(row.get("crawl_time","")),
                str(row.get("full_date",""))
            ))

        # Bulk insert
        with staging_conn.cursor() as cursor:
            cursor.executemany(f"""
                INSERT INTO {target_table} (
                    product_name, brand_name, price, old_price, discount_percent,
                    additional_info, image_url, product_url,
                    source_name, source_url, crawl_date, crawl_time, full_date
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, insert_rows)
            staging_conn.commit()
        rows_inserted = len(insert_rows)
        print(f"✅ Đã load {rows_inserted} dòng vào {target_table}.")

    except Exception as e:
        print("❌ Lỗi load dữ liệu:", traceback.format_exc())
    finally:
        staging_conn.close()
    return rows_inserted

# ==============================
# 🎯 RUN JOB DYNAMIC
# ==============================
def run_job_load_staging(job_name="LOAD_STG_CELLPHONES"):
    """
    Load CSV mới nhất vào STAGING dựa trên config job.
    - Log bắt đầu
    - Lấy file CSV mới nhất trong folder source
    - Load vào target table
    - Log kết thúc
    """
    print(f"\n🚀 Bắt đầu job: {job_name} lúc {datetime.now().strftime('%H:%M:%S')}")

    # Log bắt đầu job
    control_conn = get_control_connection()
    log_id = log_job_start(control_conn, job_name) if control_conn else None

    try:
        # Lấy config
        job_conf = get_config(job_name)
        if not job_conf:
            if control_conn and log_id:
                log_job_end(control_conn, log_id, "DISABLED", "Job disabled in config")
                update_job_status(control_conn, job_name, "DISABLED")
                control_conn.close()
            print(f"⚠️ Job {job_name} không tìm thấy config hoặc đang inactive.")
            return None

        # Lấy folder CSV từ config
        csv_folder = job_conf["source"]
        target_table = job_conf["target"]

        # Chuẩn hóa đường dẫn Windows
        csv_folder = os.path.normpath(csv_folder)

        # Kiểm tra folder tồn tại
        if not os.path.exists(csv_folder):
            msg = f"❌ Thư mục không tồn tại: {csv_folder}"
            print(msg)
            if control_conn and log_id:
                log_job_end(control_conn, log_id, "FAILED", msg)
                update_job_status(control_conn, job_name, "FAILED")
                control_conn.close()
            return None

        # Lấy file CSV mới nhất
        files = [f for f in os.listdir(csv_folder) if f.lower().endswith(".csv")]
        if not files:
            msg = f"⚠️ Không có file CSV trong thư mục {csv_folder}."
            print(msg)
            if control_conn and log_id:
                log_job_end(control_conn, log_id, "EMPTY", msg)
                update_job_status(control_conn, job_name, "EMPTY")
                control_conn.close()
            return None

        latest_file = sorted(files)[-1]  # Chọn file mới nhất theo tên
        csv_path = os.path.join(csv_folder, latest_file)

        # Load dữ liệu vào STAGING
        rows_loaded = load_to_staging(csv_path, target_table)

        # Log thành công
        msg = f"Đã load {rows_loaded} dòng vào {target_table}, từ file {csv_path}"
        print(f"✅ {msg}")
        if control_conn and log_id:
            log_job_end(control_conn, log_id, "SUCCESS", msg)
            update_job_status(control_conn, job_name, "SUCCESS")
            control_conn.close()

        return csv_path

    except Exception as e:
        error_message = traceback.format_exc()
        print(f"❌ Lỗi trong job {job_name}:", error_message)
        if control_conn and log_id:
            log_job_end(control_conn, log_id, "FAILED", "Job failed, xem error_log")
            update_job_status(control_conn, job_name, "FAILED")
        if control_conn:
            control_conn.close()
        return None

# ==============================
# 🌟 LOAD CSV CỦA NGÀY NHẬP
# ==============================
def run_job_load_staging_input_days_ago(job_name="LOAD_STG_CELLPHONES", days_ago=0):
    """
    Load dữ liệu vào STAGING từ file CSV cách 'days_ago' ngày.
    Ví dụ: days_ago=0 → hôm nay, days_ago=3 → 3 ngày trước.
    """
    print(f"\n🚀 Bắt đầu job: {job_name} lúc {datetime.now().strftime('%H:%M:%S')}")

    control_conn = get_control_connection()
    log_id = log_job_start(control_conn, job_name) if control_conn else None

    try:
        # === 1️⃣ Xác định ngày cần load ===
        target_date = (datetime.today() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        print(f"📅 Đang tìm file CSV cho ngày: {target_date}")

        # === 2️⃣ Thư mục chứa CSV ===
        crawl_folder = os.path.normpath("StagingArea/crawl_data/cellphones")
        if not os.path.exists(crawl_folder):
            msg = f"❌ Thư mục không tồn tại: {crawl_folder}"
            print(msg)
            if control_conn and log_id:
                log_job_end(control_conn, log_id, "FAILED", msg)
                update_job_status(control_conn, job_name, "FAILED")
            return None

        # === 3️⃣ Lọc file CSV đúng ngày ===
        csv_files = [f for f in os.listdir(crawl_folder)
                     if f.lower().endswith(".csv") and target_date in f]

        if not csv_files:
            msg = f"⚠️ Không tìm thấy file CSV cho ngày {target_date}"
            print(msg)
            if control_conn and log_id:
                log_job_end(control_conn, log_id, "EMPTY", msg)
                update_job_status(control_conn, job_name, "EMPTY")
            return None

        # === 4️⃣ Load file đầu tiên khớp ngày ===
        csv_path = os.path.join(crawl_folder, sorted(csv_files)[0])
        print(f"📄 Tìm thấy file: {csv_path}")

        # === 5️⃣ Lấy cấu hình job ===
        job_conf = get_config(job_name)
        if not job_conf:
            msg = f"⚠️ Không tìm thấy config cho job {job_name}"
            print(msg)
            if control_conn and log_id:
                log_job_end(control_conn, log_id, "DISABLED", msg)
                update_job_status(control_conn, job_name, "DISABLED")
            return None

        target_table = job_conf["target"]

        # === 6️⃣ Load vào STAGING ===
        rows_loaded = load_to_staging(csv_path, target_table)
        msg = f"✅ Đã load {rows_loaded} dòng vào {target_table} từ file {os.path.basename(csv_path)}"
        print(msg)

        # === 7️⃣ Ghi log thành công ===
        if control_conn and log_id:
            log_job_end(control_conn, log_id, "SUCCESS", msg)
            update_job_status(control_conn, job_name, "SUCCESS")

        return csv_path

    except Exception as e:
        error_message = traceback.format_exc()
        print(f"❌ Lỗi trong job {job_name}:\n{error_message}")
        if control_conn and log_id:
            log_job_end(control_conn, log_id, "FAILED", "Job failed - xem error_log")
            update_job_status(control_conn, job_name, "FAILED")
        return None

    finally:
        if control_conn:
            control_conn.close()

# ==============================
# 🌟 MAIN CHẠY JOB THEO NGÀY NHẬP
# ==============================
def main():
    """
    Main chạy job load STAGING theo ngày nhập từ người dùng.
    """
    date_input = input("Nhập ngày cần load ETL (YYYY-MM-DD): ").strip()

    # Kiểm tra định dạng ngày
    try:
        target_date = datetime.strptime(date_input, "%Y-%m-%d").date()
    except ValueError:
        print("⚠️ Định dạng ngày không hợp lệ. Vui lòng nhập theo YYYY-MM-DD.")
        return

    # Tính số ngày so với hôm nay
    today = datetime.today().date()
    days_ago = (today - target_date).days

    if days_ago < 0:
        print(f"⚠️ Ngày {date_input} chưa tới, không thể load trước thời gian này.")
        return

    print(f"📅 Load dữ liệu cho ngày {date_input} (days_ago={days_ago})")

    try:
        csv_path = run_job_load_staging_input_days_ago(days_ago=days_ago)
        if csv_path:
            print(f"✅ ETL load thành công file: {csv_path}")
        else:
            print("⚠️ Không load được file CSV. Vui lòng kiểm tra lại thư mục crawl_data.")
    except Exception as e:
        print(f"❌ Lỗi khi chạy ETL cho ngày {date_input}:\n{traceback.format_exc()}")

# ==============================
# 🔹 MAIN
# ==============================
if __name__ == "__main__":
    # run_job_load_staging()  # Chạy job load CSV mới nhất
    # run_job_load_staging_input_days_ago # Lỗi: không có () để gọi hàm
    main()  # Chạy job theo ngày nhập từ người dùng
