
import  os, traceback
import test_connection as cn
import pymysql
from Extract2 import crawl_celphones,save_to_csv_cellphones
from Extract3 import crawl_tgdd,save_to_csv_tgdd
from Extract4 import crawl_hoanghamobile,save_to_csv_hoanghamobile
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# ============================================
# 🔌 KẾT NỐI DATABASE CONTROL
# ============================================
def get_connection():
    conn = cn.get_control_connection()
    if not conn:
        print("❌ Không thể kết nối tới database CONTROL.")
    return conn


def update_job_status(job_name, status):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO control.job_status (job_name, status, last_run)
                VALUES (%s, %s, NOW())
                AS new
                ON DUPLICATE KEY UPDATE
                    status = new.status,
                    last_run = new.last_run
            """, (job_name, status))
        conn.commit()
    finally:
        conn.close()




# LẤY CẤU HÌNH JOB TỪ BẢNG config
def get_job_config(job_name):
    conn = get_connection()
    if not conn:
        return None

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT job_name, source, target, schedule_time, active
                FROM config
                WHERE job_name = %s
            """, (job_name,))
            config = cursor.fetchone()
        conn.close()

        if not config:
            print(f" Không tìm thấy cấu hình cho job: {job_name}")
            return None
        if config["active"] != 1:
            print(f" Job {job_name} đang bị tắt (active=0). Dừng chạy.")
            return None

        print(f" Đã đọc config cho job: {job_name}")
        return config

    except Exception as e:
        print(" Lỗi khi đọc config:", e)
        return None
#  GHI LOG BẮT ĐẦU JOB
def insert_job_log_start(job_name):
    conn = get_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO job_log (job_name, start_time, status, message)
                VALUES (%s, NOW(), 'RUNNING', 'Job started')
            """, (job_name,))
            conn.commit()
            
            log_id = cursor.lastrowid 
        return log_id
    except Exception as e:
        print(" Lỗi khi insert job_log start:", e)
        return None
    finally:
        conn.close()
        
# CẬP NHẬT LOG KẾT THÚC JOB
def update_job_log_end(log_id, status="SUCCESS", message="Job completed successfully"):
    if not log_id:
        return
    conn = get_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE job_log
                SET end_time = NOW(),
                    status = %s,
                    message = %s
                WHERE job_id = %s
            """, (status, message, log_id))
            conn.commit()
    except Exception as e:
        print(" Lỗi khi update job_log end:", e)
    finally:
        conn.close()


def get_job_status(job_name):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT status 
                FROM control.job_status 
                WHERE job_name = %s
            """, (job_name,))
            row = cursor.fetchone()
            return row["status"] if row else None

    finally:
        conn.close()


def send_mail(subject, body, to_emails,attachment_path):

    # Cấu hình email
    smtp_server = "smtp.gmail.com"     
    smtp_port = 587
    sender_email = "tquockhanh009@gmail.com"
    sender_password = "uzdmyecqebvxzzzv"  

    # Tạo nội dung email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(to_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # === File đính kèm ===
    try:
        with open(attachment_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
        msg.attach(part)
    except:
        print(" Không thể đính kèm file CSV vào email!")

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, to_emails, msg.as_string())
        server.quit()
        print(" Email gửi thành công kèm file CSV")
    except Exception as e:
        print(f" Lỗi gửi email: {e}")


    
# crawl cellphones
def run_crawl_cellphones():
    job_name = "Crawl_Cellphones"
    to_emails = ["tquockhanh009@gmail.com"]

    print(f"🚀 Bắt đầu job: {job_name}")
    # 1.  Lấy cấu hình job
    config = get_job_config(job_name)
    if not config:
        msg = " Không tìm thấy cấu hình job trong control.job_config"
        print(msg)
        send_mail(f"[{job_name}] FAILED - Missing Config", msg, to_emails)
        return
    # 2.Tạo log id
    log_id = insert_job_log_start(job_name)

    try:
        print("🔍 Đang crawl dữ liệu Cellphones...")
        # 3. Lấy danh sách URL từ bảng config
        source_raw = config["source"]
        url_list = [url.strip() for url in source_raw.split(";") if url.strip()]
        # 4.Crawl dữ liệu
        products = crawl_celphones(url_list)
        # 5. Không có dữ liệu → FAIL
        if not products:
            msg = "Crawl hoàn thành nhưng không thu được dữ liệu."
            print(msg)
            update_job_status(job_name, "FAILED")
            update_job_log_end(log_id, status="FAILED", message=msg)
            send_mail(f"[{job_name}] FAILED - No data", msg, to_emails)
            return
         # 6. Lưu CSV
        output_dir = config["target"].replace("\\", "/").strip()
        os.makedirs(output_dir, exist_ok=True)
        output_file = save_to_csv_cellphones(products, output_dir)

        # 7. Cập nhật trạng thái SUCCESS
        
        update_job_status(job_name, "SUCCESS")
        # 8. Cập nhật status Job_log
        success_msg = f"{len(products)} sản phẩm được lưu: {output_file}"
        update_job_log_end(log_id, status="SUCCESS", message=success_msg)

        # 9. Gửi email SUCCESS + đính kèm file CSV
        body = (
            f" Job {job_name} chạy thành công.\n"
            f" Số sản phẩm: {len(products)}\n"
            f" File: {output_file}"
        )
        send_mail(
            subject=f"[{job_name}] SUCCESS",
            body=body,
            to_emails=to_emails,
            attachment_path=output_file
        )

    except Exception as e:
        # . Bắt lỗi toàn bộ → gửi mail FAIL
        err_msg = f" Lỗi khi chạy job: {str(e)}\n{traceback.format_exc()}"
        print(err_msg)

        update_job_status(job_name, "FAILED")
        update_job_log_end(log_id, status="FAILED", message=err_msg)

        send_mail(f"[{job_name}] FAILED - Exception", err_msg, to_emails)



# crawl thế giới di động
def run_crawl_tgdd():
    job_name = "Crawl_tgdd"
    to_emails = ["tquockhanh009@gmail.com"]
    print(f"🚀 Bắt đầu job: {job_name}")
    # 1. Lấy cấu hình job
    config = get_job_config(job_name)
    if not config:
        msg = " Không tìm thấy cấu hình job trong control.job_config"
        print(msg)
        
        send_mail(f"[{job_name}] FAILED - Missing Config", msg, to_emails)
        return
    # 2.Tạo log id
    log_id = insert_job_log_start(job_name)
    try:
        print("🔍 Đang crawl dữ liệu Thế Giới Di Động...")
        # 3. Lấy danh sách URL từ bảng config
        source_raw = config["source"]
        url_list = [url.strip() for url in source_raw.split(";") if url.strip()]
        # 4. Crawl dữ liệu
        products = crawl_tgdd(url_list)
        # 5. Không có dữ liệu -> FAIL
        if not products:
            msg = " Crawl hoàn thành nhưng không thu được dữ liệu."
            print(msg)
            update_job_status(job_name, "FAILED")
            update_job_log_end(log_id, status="FAILED", message=msg)
            send_mail(f"[{job_name}] FAILED", msg, to_emails)
            return

        # 6.  Lưu CSV
        output_dir = config["target"].replace("\\", "/").strip()
        os.makedirs(output_dir, exist_ok=True)
        output_file = save_to_csv_tgdd(products, output_dir)

        # 5. Cập nhật trạng thái SUCCESS
        success_msg = f"{len(products)} sản phẩm được lưu: {output_file}"
        update_job_status(job_name, "SUCCESS")
        update_job_log_end(log_id,status="SUCCESS",message=success_msg)
        # 6. Gửi email SUCCESS + đính kèm file CSV
        body = (
            f"Job {job_name} chạy thành công.\n"
            f"Số sản phẩm crawl được: {len(products)}\n"
            f"File: {output_file}"
        )
        send_mail(
            subject=f"[{job_name}] SUCCESS",
            body=body,
            to_emails=to_emails,
            attachment_path=output_file
        )
    except Exception as e:
        # 7.  Bắt lỗi toàn bộ -> gửi mail FAIL
        err_msg = f" Lỗi khi chạy job: {str(e)}\n{traceback.format_exc()}"
        print(err_msg)
        update_job_status(job_name, "FAILED")
        update_job_log_end(log_id, status="FAILED", message=err_msg)
        send_mail(f"[{job_name}] FAILED - Exception", err_msg, to_emails)





        
# crawl Hoàng hà mobile
def run_crawl_hoanghamobile():
    job_name = "Crawl_hoanghamobile"
    to_emails = ["tquockhanh009@gmail.com"]

    print(f"🚀 Bắt đầu job: {job_name}")

    # 1. Lấy cấu hình job
    config = get_job_config(job_name)
    if not config:
        msg = " Không tìm thấy cấu hình job trong bảng control.job_config"
        print(msg)

        send_mail(f"[{job_name}] FAILED - Missing Config", msg, to_emails)
        return

    # 2.Tạo log id
    log_id = insert_job_log_start(job_name)

    try:
        print("🔍 Đang crawl dữ liệu Hoàng Hà Mobile...")

        # 3. Lấy danh sách URL từ bảng config
        source_raw = config["source"]
        url_list = [url.strip() for url in source_raw.split(";") if url.strip()]

        # 4. Crawl dữ liệu
        products = crawl_hoanghamobile(url_list)

        # 5. Không có dữ liệu → FAIL
        if not products:
            msg = " Crawl hoàn thành nhưng không thu được dữ liệu."
            print(msg)

            update_job_status(job_name, "FAILED")
            update_job_log_end(log_id, status="FAILED", message=msg)

            send_mail(f"[{job_name}] FAILED - No data", msg, to_emails)
            return

        # 6. Lưu CSV
        output_dir = config["target"].replace("\\", "/").strip()
        os.makedirs(output_dir, exist_ok=True)
        output_file = save_to_csv_hoanghamobile(products, output_dir)
        # 7. Cập nhật job status
        update_job_status(job_name, "SUCCESS")
        # 8. Cập nhật status Job_log
        success_msg = f"{len(products)} sản phẩm được lưu: {output_file}"
        update_job_log_end(log_id, status="SUCCESS", message=success_msg)

        # 9. Gửi email SUCCESS + đính kèm file CSV
        body = (
            f" Job {job_name} chạy thành công!\n"
            f" Số sản phẩm: {len(products)}\n"
            f" File: {output_file}"
        )

        send_mail(
            subject=f"[{job_name}] SUCCESS",
            body=body,
            to_emails=to_emails,
            attachment_path=output_file
        )

    except Exception as e:
        #  Bắt lỗi toàn bộ → gửi mail FAIL
        err_msg = f" Lỗi khi chạy job: {str(e)}\n{traceback.format_exc()}"
        print(err_msg)

        update_job_status(job_name, "FAILED")
        update_job_log_end(log_id, status="FAILED", message=err_msg)

        send_mail(f"[{job_name}] FAILED - Exception", err_msg, to_emails)



# CHẠY SINGLE JOB THEO TÊN
def run_single_job(job_name):
    job_map = {
    "crawl_cellphones": run_crawl_cellphones,
    "crawl_tgdd": run_crawl_tgdd,
    "crawl_hoanghamobile": run_crawl_hoanghamobile
    }


    job_func = job_map.get(job_name.lower())
    if not job_func:
        print(f" Job {job_name} không có hàm tương ứng, bỏ qua.")
        return

    print(f"\n==============================")
    print(f" BẮT ĐẦU JOB: {job_name}")
    print("==============================")

    # LẦN 1
    try:
        job_func()
        update_job_status(job_name, "SUCCESS")
        print(f"    Lần 1: {job_name} SUCCESS")
        return
    except Exception as e:
        update_job_status(job_name, "FAILED")
        print(f"    Lần 1: {job_name} FAILED: {e}")

    # RETRY
    print(f" Retry job: {job_name}")
    try:
        job_func()
        update_job_status(job_name, "SUCCESS")
        print(f"    Retry: {job_name} SUCCESS")
    except Exception as e:
        update_job_status(job_name, "FAILED")
        print(f"    Retry: {job_name} FAILED lần 2: {e}")
        print(f"    Bỏ qua job này và chuyển tiếp.")



# RUN JOB EXTRACT TUẦN TỰ
def run_job_extract():
    jobs = ["crawl_cellphones", "crawl_tgdd", "crawl_hoanghamobile"]

    print(" Chạy ETL Extract tuần tự (quản lý trạng thái tại run_job_extract)...")

    for job_name in jobs:
        run_single_job(job_name)

    print("\n Tất cả job extract đã xử lý xong (có retry).")
    
if __name__ == "__main__":
    run_job_extract()
    #run_crawl_hoanghamobile() 

   