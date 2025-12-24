import requests, json
from datetime import date, datetime, timedelta
import pandas as pd

def extract_data_from_api(**kwargs):
    
    list_of_states=list(requests.get(
        "https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json").json().keys())

    size= 500
    time_delta=30
    max_date = (date.today()).strftime("%Y-%m-%d")
    min_date = (date.today() - timedelta(days=time_delta)).strftime("%Y-%m-%d")
    
    all_data = []
    
    for state in list_of_states:
        
        headers = {
            'accept': 'application/json',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/139.0.0.0 Safari/537.36"
        }

        params = {
            'field': 'complaint_what_happened',
            'size': size,
            'date_received_max': max_date,
            'date_received_min': min_date,
            'state': state,
        }

        response = requests.get(
            'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/',
            params=params,
            headers=headers,
        )
        
        if response.status_code == 200:
            data = response.json()
            main_data = data["hits"]["hits"]
            
            for content in main_data:
                all_data.append(content["_source"])
            
        else: 
            print(f"The customer complaint data for {state} is not found")
            continue
                
    complete_data = pd.DataFrame(all_data)
    file_path = "/home/rubas/airflow/dags/Assignment_week4/consumer_complaints_raw.csv"
    complete_data.to_csv(file_path, index=False)

    return file_path
