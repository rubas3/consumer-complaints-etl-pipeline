from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

MYSQL_CONN_ID = 'store_data_mysql'

def data_manipulation():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    sql = "SELECT * FROM Consumer_Complaints"
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql(sql, engine)
        
    cols_to_drop = [
        "complaint_what_happened",
        "date_sent_to_company",
        "zip_code",
        "tags",
        "has_narrative",
        "consumer_consent_provided",
        "consumer_disputed",
        "company_public_response"
    ]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    df["month_year"] = pd.to_datetime(df["date_received"], errors="coerce").dt.strftime("%B-%Y")

    df["month_year"] = pd.to_datetime(df["date_received"], errors="coerce").dt.strftime("%b-%Y")
    df = df.drop(columns=["date_received"], errors="ignore")

    df = df.fillna("")

    group_dims = [
        "product", "issue", "sub_product", "sub_issue", "timely", 
        "company_response", "submitted_via", "company", "state", "month_year"
    ]

    transformed_df = (
        df.groupby(group_dims, dropna=False)
          .agg(Count_of_complaint_id=("complaint_id", "nunique"))
          .reset_index()
    )

    transformed_df.to_csv("/home/rubas/airflow/dags/Assignment_week4/consumer_complaints_transformed.csv", index=False)

