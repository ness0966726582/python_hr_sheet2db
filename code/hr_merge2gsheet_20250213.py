import os
import psycopg2
import gspread
from datetime import datetime
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# 加載 .env 文件中的環境變數
load_dotenv()

# DB 資訊
POSTGRES_SERVER = os.getenv('N_POSTGRES_SERVER')
POSTGRES_DB = os.getenv('N_POSTGRES_DB')
POSTGRES_USER = os.getenv('N_POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('N_POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('N_POSTGRES_PORT')

# Google Sheets API 資訊 auto-update@pbg-it.iam.gserviceaccount.com
#https://docs.google.com/spreadsheets/d/1veNclH-62PWTKaUwi7UNeP_lPM4nKunFNpbF24XCmGc/edit?gid=1170000688#gid=1170000688
my_spreadsheet_id = "1veNclH-62PWTKaUwi7UNeP_lPM4nKunFNpbF24XCmGc"  # 替換為您的試算表 ID
my_Googlesheet_PageName = "Merge"                                   #員工彙整表
#my_Googlesheet_PageName = "(Merge) Department Code"                #部門代號表


# 設定 Google Sheets API 認證
# 機器人 auto-update@pbg-it.iam.gserviceaccount.com

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)   #/opt/airflow/dags/hr/cred.json
client = gspread.authorize(creds)

# 批次處理大小
BATCH_SIZE = 800

# 確認 Google Sheet 的資料
def check_google_sheet():
    sheet = client.open_by_key(my_spreadsheet_id).worksheet(my_Googlesheet_PageName)
    titles = sheet.row_values(1)  # 取得第一行的標題
    total_rows = sheet.row_count  # 總行數
    print(f"標題: {titles}")
    print(f"總行數: {total_rows}")
    return total_rows

# 建立資料表
def create_table_if_not_exists(conn):
    with conn.cursor() as cursor:
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS hr_merge_for_IT_use (
                div VARCHAR(13),
                last_name VARCHAR(50),
                first_name VARCHAR(50),
                middle_name VARCHAR(13),
                formal_name VARCHAR(255),
                department VARCHAR(50),
                cost_centre VARCHAR(50),
                reporting_date DATE,
                resigned_date DATE,
                "10_number" VARCHAR(10) PRIMARY KEY,
                type VARCHAR(21),
                department_code VARCHAR(15),
                cost_centre_code VARCHAR(16),
                transfer_record VARCHAR(50),
                remark VARCHAR(87),
                card_number VARCHAR(59),
                adm_remark VARCHAR(94),
                active VARCHAR(6)
            );
        """)
        conn.commit()

# 批量插入或更新資料
def upsert_data(sheet, total_rows, conn):
    with conn.cursor() as cursor:
        for start in range(2, total_rows + 1, BATCH_SIZE):
            end = min(start + BATCH_SIZE - 1, total_rows)
            batch_data = sheet.get(f"A{start}:S{end}")

            for idx, row in enumerate(batch_data, start=start):
                # 檢查每行的資料長度
                if len(row) < 20:
                    print(f"行 {idx} 資料不足，長度為 {len(row)}: {row}")
                    while len(row) < 20:
                        row.append('')  # 使用空字串補齊

                # 處理空字串的日期欄位
                reporting_date = row[7] if row[7] else None
                resigned_date = row[8] if row[8] else None

                # 確保日期格式正確，轉換為 YYYY-MM-DD 格式
                if reporting_date:
                    try:
                        reporting_date = datetime.strptime(reporting_date, '%Y/%m/%d').strftime('%Y-%m-%d')
                    except ValueError:
                        reporting_date = None  # 日期格式錯誤時設為 None

                if resigned_date:
                    try:
                        resigned_date = datetime.strptime(resigned_date, '%Y/%m/%d').strftime('%Y-%m-%d')
                    except ValueError:
                        resigned_date = None  # 日期格式錯誤時設為 None

                # 假設 row[9] 是 "10_number" 欄位
                if row[9]:  
                    cursor.execute("""
                        INSERT INTO hr_merge_for_IT_use (
                            div, last_name, first_name, middle_name, formal_name, 
                            department, cost_centre, reporting_date, resigned_date, 
                            "10_number", type, department_code, cost_centre_code, 
                            transfer_record, remark, card_number, adm_remark, active
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ("10_number") DO UPDATE SET
                            div = EXCLUDED.div,
                            last_name = EXCLUDED.last_name,
                            first_name = EXCLUDED.first_name,
                            middle_name = EXCLUDED.middle_name,
                            formal_name = EXCLUDED.formal_name,
                            department = EXCLUDED.department,
                            cost_centre = EXCLUDED.cost_centre,
                            reporting_date = EXCLUDED.reporting_date,
                            resigned_date = EXCLUDED.resigned_date,
                            type = EXCLUDED.type,
                            department_code = EXCLUDED.department_code,
                            cost_centre_code = EXCLUDED.cost_centre_code,
                            transfer_record = EXCLUDED.transfer_record,
                            remark = EXCLUDED.remark,
                            card_number = EXCLUDED.card_number,
                            adm_remark = EXCLUDED.adm_remark,
                            active = EXCLUDED.active
                    """, (
                        row[0], row[1], row[2], row[3], row[4], 
                        row[5], row[6], reporting_date, resigned_date, 
                        row[9], row[10], row[11], row[12], 
                        row[13], row[14], row[15], row[16], row[19]
                    ))

            conn.commit()
            print(f"已處理行數: {end - start + 1}")


if __name__ == "__main__":
    # 連接到 PostgreSQL 資料庫
    conn = psycopg2.connect(
        host=POSTGRES_SERVER,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )

    try:
        total_rows = check_google_sheet()
        create_table_if_not_exists(conn)
        upsert_data(client.open_by_key(my_spreadsheet_id).worksheet(my_Googlesheet_PageName), total_rows, conn)
    finally:
        conn.close()
