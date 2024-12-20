==============================
方法1.個別安裝
==============================
1. 安裝 Airflow
> pip install apache-airflow

2. 安裝 PostgreSQL 相關庫
> pip install psycopg2

3. 安裝 Google Sheets API 相關庫
> pip install gspread oauth2client

4.如果你在 .env 文件中管理環境變數，還需要 python-dotenv：
pip install python-dotenv

==============================
方法2.一次安裝
==============================
如果你的 Airflow 是透過 Docker 部署的，可以將這些庫加入到你的 Dockerfile 或通過 requirements.txt 安裝。
使用 requirements.txt 安裝

步驟一

可以創建一個 requirements.txt 文件，包含你需要的庫：
psycopg2
gspread
oauth2client
python-dotenv

步驟二
然後在 Airflow 環境中使用以下命令來安裝：
pip install -r requirements.txt
