import pymysql
import yaml
import os

# ĐỌC FILE CONFIG
def load_config():
    """
    Đọc file config.yaml
    """
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)

config = load_config()

# HÀM KẾT NỐI CHUNG
def connect_to_db(db_name_key):
    """
    Kết nối đến DB theo key trong config.yaml
    """
    try:
        db_name = config["databases"][db_name_key]
        conn = pymysql.connect(
            host=config["mysql"]["host"],
            port=config["mysql"]["port"],
            user=config["mysql"]["user"],
            password=config["mysql"]["password"],
            database=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        print(f"✅ Kết nối thành công tới DB: {db_name}")
        return conn
    except pymysql.MySQLError as e:
        print(f"❌ Lỗi MySQL ({db_name_key}):", e)
        return None

# HÀM TIỆN ÍCH
def get_staging_connection():
    return connect_to_db("staging")

def get_control_connection():
    return connect_to_db("control")

def get_dw_connection():
    return connect_to_db("dw")


# TEST KẾT NỐI
if __name__ == "__main__":
    for db_key in ["staging", "control", "dw"]:
        print(f"\n--- Kiểm tra {db_key} ---")
        conn = connect_to_db(db_key)
        if conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DATABASE();")
                print("📂 Database:", cursor.fetchone()["DATABASE()"])
            conn.close()
