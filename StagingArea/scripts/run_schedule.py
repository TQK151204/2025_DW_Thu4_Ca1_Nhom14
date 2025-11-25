import schedule
import time
from datetime import datetime
from Extract import run_single_job, run_job_extract  # import các hàm ETL
from Extract  import get_connection
import pymysql

# Lấy tất cả job active từ DB
def get_active_jobs():
    conn = get_connection()
    if not conn:
        return []
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT job_name, schedule_time
                FROM config
                WHERE active = 1
            """)
            jobs = cursor.fetchall()
        return jobs
    except Exception as e:
        print(f"Lỗi khi đọc active jobs: {e}")
        return []
    finally:
        conn.close()


# Đăng ký các job với schedule
def schedule_jobs():
    jobs = get_active_jobs()
    if not jobs:
        print("Không có job active nào để schedule.")
        return

    for job in jobs:
        job_name = job["job_name"]
        td = job["schedule_time"]
        schedule_time = f"{td.seconds//3600:02d}:{(td.seconds//60)%60:02d}"

        # Map tên job → hàm crawl tương ứng
        job_map = {
            "crawl_cellphones": lambda: run_single_job("Crawl_Cellphones"),
            "crawl_tgdd": lambda: run_single_job("Crawl_tgdd"),
            "crawl_hoanghamobile": lambda: run_single_job("Crawl_hoanghamobile"),
            "all_extract": run_job_extract
        }

        job_name_lower = job_name.lower()
        job_func = job_map.get(job_name_lower)
        if job_func:
            schedule.every().day.at(schedule_time).do(job_func)
            print(f" Job {job_name} sẽ chạy lúc {schedule_time} mỗi ngày")
        else:
            print(f" Job {job_name} chưa có hàm tương ứng, bỏ qua.")


# Vòng lặp chạy scheduler
if __name__ == "__main__":
    schedule_jobs()
    print("Scheduler ETL đã thiết lập. Nhấn Ctrl+C để dừng thủ công.")
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            print(" Scheduler đã dừng thủ công.")
            break
