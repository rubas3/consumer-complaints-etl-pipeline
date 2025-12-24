from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

MYSQL_CONN_ID = 'store_data_mysql'

def load_xcom_save_mysql(ti):
    data_path = ti.xcom_pull('extract_data_from_source', key='return_value')

    extracted_data = pd.read_csv(data_path)

    extracted_data = extracted_data.where(pd.notnull(extracted_data), '')
    
    hook = MySqlHook(mysql_conn_id = MYSQL_CONN_ID)

    hook.run("DROP TABLE IF EXISTS Consumer_Complaints;")

    hook.run("""
        CREATE TABLE Consumer_Complaints (
           id INT AUTO_INCREMENT PRIMARY KEY,
            state VARCHAR(20),
            product VARCHAR(255),
            complaint_what_happened TEXT,
            date_sent_to_company DATETIME,
            issue VARCHAR(255),
            sub_product VARCHAR(255),
            zip_code VARCHAR(20),
            tags VARCHAR(255),
            has_narrative BOOLEAN,
            complaint_id VARCHAR(50),
            timely VARCHAR(10),
            consumer_consent_provided VARCHAR(50),
            company_response VARCHAR(255),
            submitted_via VARCHAR(100),
            company VARCHAR(255),
            date_received DATETIME,
            consumer_disputed VARCHAR(10),
            company_public_response TEXT,
            sub_issue VARCHAR(255)
        );
    """)
    
    hook.insert_rows(
        table="Consumer_Complaints",
        rows=extracted_data.values.tolist(),
        target_fields=extracted_data.columns.tolist(),
        commit_every=1000
    )
            
            
def load_csv_save_google():
    SERVICE_ACCOUNT_FILE = "/home/rubas/airflow/dags/Assignment_week4/service_account.json"

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    
    spreadsheet = client.open("Consumer Complaints Data") 
   
    sheet = spreadsheet.sheet1  

    load_csv = pd.read_csv("/home/rubas/airflow/dags/Assignment_week4/consumer_complaints_transformed.csv")

    load_csv = load_csv.fillna('')
    
    sheet.clear()
    sheet.update([load_csv.columns.values.tolist()] + load_csv.values.tolist())

    print("✅ Data uploaded to Google Sheets successfully!")