import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# DB 資訊
my_serverIP = "10.231.220.60"
my_DBName = "etlv1"
my_login_userName = "postgres"
my_login_password = ""
my_port = "5432"

# Google Sheets API 資訊
my_spreadsheet_name = "引用-HR 10碼工號"
my_Googlesheet_PageName = "工作表1"

# 設定 Google Sheets API 認證
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)
client = gspread.authorize(creds)

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
    header = [f'col_{i+1}' if col == '' else col for i, col in enumerate(header)]

    # 連接到 PostgreSQL 資料庫
    conn = psycopg2.connect(
        host=my_serverIP,
        port=my_port,
        dbname=my_DBName,
        user=my_login_userName,
        password=my_login_password
    )
    cursor = conn.cursor()

    # 刪除已存在的表格（如果存在）
    cursor.execute('DROP TABLE IF EXISTS employee_records_for_IT_use')
    conn.commit()
    print("已刪除表格 employee_records_for_IT_use（如果存在）")

    # 創建新表格
    create_table_query = '''
    CREATE TABLE employee_records_for_IT_use (
        "Div" VARCHAR(50),
        "Formal Name" VARCHAR(100),
        "Department" VARCHAR(100),
        "Cost Centre" VARCHAR(50),
        "Reporting date" DATE,
        "Resigned date" DATE,
        "10 Number" VARCHAR(10)  ,
        "Department Code" VARCHAR(8),
        "Cost Centre Code" VARCHAR(8)
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    print("表格 employee_records_for_IT_use 已成功創建")

    # 根據 Google Sheets 的資料結構插入資料
    columns = [
        "Div", "Formal Name", "Department", "Cost Centre", 
        "Reporting date", "Resigned date", "10 Number", 
        "Department Code", "Cost Centre Code"
    ]
    columns_str = ', '.join(f'"{col}"' for col in columns)
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f'INSERT INTO employee_records_for_IT_use ({columns_str}) VALUES ({placeholders})'

    for row in rows:
        # Match each row with corresponding columns
        record = [row[header.index(col)] if col in header else None for col in columns]
        
        # Skip the row if "Div" is empty
        if not record[0]:
            print(f"跳過 'Div' 欄位為空的行: {record}")
            continue
        
        # Convert invalid date values to None
        def clean_date(date_str):
            if date_str and date_str != '-' and len(date_str) == 10:
                return date_str
            return None

        record[4] = clean_date(record[4])  # Reporting date
        record[5] = clean_date(record[5])  # Resigned date

        try:
            cursor.execute(insert_query, record)
        except psycopg2.IntegrityError:
            conn.rollback()  # Skip the duplicate
            print(f"跳過重複的 '10 Number': {record[6]}")
        except psycopg2.DataError as e:
            print(f"跳過無效的資料: {record}, 錯誤: {e}")
            conn.rollback()  # Skip the invalid data
        else:
            conn.commit()

    print("資料已成功上傳至 PostgreSQL 資料庫")

except gspread.SpreadsheetNotFound:
    print("找不到指定的 Google Sheets 文件。請確保文件名稱正確。")
except gspread.WorksheetNotFound:
    print("找不到指定的分頁。請確保分頁名稱正確。")
except psycopg2.Error as e:
    print(f"發生錯誤: {e}")
    conn.rollback()
finally:
    # 關閉連接
    cursor.close()
    conn.close()
