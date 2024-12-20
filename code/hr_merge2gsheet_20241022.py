from dotenv import load_dotenv
import os
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# 加載 .env 文件中的環境變數
load_dotenv()

# DB 資訊
POSTGRES_SERVER = os.getenv('N_POSTGRES_SERVER')
POSTGRES_DB = os.getenv('N_POSTGRES_DB')
POSTGRES_USER = os.getenv('N_POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('N_POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('N_POSTGRES_PORT')

# Google Sheets API 資訊
my_spreadsheet_name = "引用-HR 10碼工號"
my_Googlesheet_PageName = "工作表1"

# 設定 Google Sheets API 認證
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)
client = gspread.authorize(creds)

def parse_date(date_str):
    """
    嘗試將日期字串解析為 'YYYY-MM-DD' 格式。
    如果日期是空的，返回 'NA'。
    """
    if not date_str or date_str == '-':
        return 'NA'
    
    date_formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d-%m-%Y',
        '%Y/%m/%d',
        '%d/%m/%Y',
        '%m-%d-%Y',
        '%Y.%m.%d',
        '%d.%m.%Y'
        # 根據需要添加更多格式
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str.strip(), fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return 'NA'  # 所有格式均無法匹配時返回 'NA'

try:
    # 打開 Google Sheets
    spreadsheet = client.open(my_spreadsheet_name)
    sheet = spreadsheet.worksheet(my_Googlesheet_PageName)

    # 取得所有資料
    data = sheet.get_all_values()
    if not data:
        raise ValueError("Google Sheets 中沒有數據")

    header = data[0] if len(data) > 0 else []
    rows = data[1:] if len(data) > 1 else []

    # 處理空的列名
    header = [f'col_{i+1}' if col.strip() == '' else col.strip() for i, col in enumerate(header)]

    # 連接到 PostgreSQL 資料庫
    conn = psycopg2.connect(
        host=POSTGRES_SERVER,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        options='-c client_encoding=UTF8'  # 設定 PostgreSQL 連線使用 UTF-8 編碼
    )
    cursor = conn.cursor()

    # 刪除已存在的表格（如果存在）
    cursor.execute('DROP TABLE IF EXISTS employee_records_for_IT_use')
    conn.commit()
    print("已刪除表格 employee_records_for_IT_use（如果存在）")

    # 創建新表格
    create_table_query = '''
    CREATE TABLE employee_records_for_IT_use (
        "Div" VARCHAR(10),
        "Formal_Name" VARCHAR(40),
        "Department" VARCHAR(10),
        "Cost_Centre" VARCHAR(10),
        "Reporting_date" DATE,
        "Resigned_date" DATE,
        "10_Number" VARCHAR(10),
        "Department_Code" VARCHAR(8),
        "Cost_Centre_Code" VARCHAR(8)
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    print("表格 employee_records_for_IT_use 已成功創建")

    # 根據 Google Sheets 的資料結構插入資料
    columns = [
        "Div", "Formal_Name", "Department", "Cost_Centre", 
        "Reporting_date", "Resigned_date", "10_Number", 
        "Department_Code", "Cost_Centre_Code"
    ]
    columns_str = ', '.join(f'"{col}"' for col in columns)
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f'INSERT INTO employee_records_for_IT_use ({columns_str}) VALUES ({placeholders})'

    # 開啟記錄跳過部分的文字檔，指定使用 UTF-8 編碼
    skipped_file = open("skipped_records.txt", "w", encoding="utf-8")

    for row_num, row in enumerate(rows, start=2):  # start=2 表示從第二行開始（第一行是標題）
        try:
            # Match each row with corresponding columns
            record = []
            for col in columns:
                if col in header:
                    value = row[header.index(col)].strip()
                    # 如果欄位是空的，替換為 'NA'
                    if not value:
                        value = "NA"
                    # 忽略無法編碼的字符
                    value = value.encode('utf-8', 'ignore').decode('utf-8')
                else:
                    value = "NA"
                record.append(value)
            
            # 清理並解析日期欄位
            record[4] = parse_date(record[4])  # Reporting_date
            record[5] = parse_date(record[5])  # Resigned_date

            # 如果日期是 'NA'，將其替換為 None（對應 SQL 中的 NULL）
            record[4] = None if record[4] == 'NA' else datetime.strptime(record[4], '%Y-%m-%d').date()
            record[5] = None if record[5] == 'NA' else datetime.strptime(record[5], '%Y-%m-%d').date()

            cursor.execute(insert_query, record)
        except psycopg2.IntegrityError:
            conn.rollback()  # 跳過重複
            skipped_file.write(f"跳過重複的 '10_Number' 在第 {row_num} 行: {record[6]}\n")
        except psycopg2.DataError as e:
            conn.rollback()  # 跳過無效資料
            skipped_file.write(f"跳過無效的資料在第 {row_num} 行: {record}, 錯誤: {e}\n")
        except Exception as e:
            conn.rollback()
            skipped_file.write(f"處理第 {row_num} 行時發生未預期的錯誤: {record}, 錯誤: {e}\n")
        else:
            conn.commit()

    print("資料已成功上傳至 PostgreSQL 資料庫")

except gspread.SpreadsheetNotFound:
    print("找不到指定的 Google Sheets 文件。請確保文件名稱正確。")
except gspread.WorksheetNotFound:
    print("找不到指定的分頁。請確保分頁名稱正確。")
except psycopg2.Error as e:
    print(f"發生 PostgreSQL 錯誤: {e}")
    if 'conn' in locals():
        conn.rollback()
except Exception as e:
    print(f"發生未預期的錯誤: {e}")
    if 'conn' in locals():
        conn.rollback()
finally:
    # 關閉連接和檔案
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    skipped_file.close()
