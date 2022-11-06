from venv import create
from airflow import DAG
import airflow.utils.dates
from airflow.sensors.python import PythonSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

import json
import requests
import os
import pandas as pd

dag = DAG(
    dag_id="create_user_pages",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description='A batch workflow for create users notion pages.'
)

NOTION_KEY = os.getenv('NOTION_KEY')
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID')

def _get_user_names(aib_num):
    csv_path = os.path.join(os.getcwd(), f'data/aib-{aib_num}/data.csv')
    df = pd.read_csv(csv_path)
    data_list = []
    for name in df['이름']:
        data_list.append({
            "parent": {
                "database_id": f"{NOTION_DATABASE_ID}"
            },
            "properties": {
                "title": [
                    {
                        "text": {
                            "content": name
                        }
                    }
                ]
            }
        })
    with open(f"data/aib-{aib_num}/data.json", 'w', encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False)

def _create_user_pages(**context):
    aib_num = context["templates_dict"]['aib_num']
    url = 'https://api.notion.com/v1/pages'
    headers = {
        "Authorization": f"Bearer {NOTION_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    
    with open(f'data/aib-{aib_num}/data.json', 'r', encoding="utf-8") as f:
        data_list = json.load(f)
    
    for data in data_list:
        response = requests.post(url, headers=headers, json=data)

wait_for_user_data = FileSensor(
    task_id="wait_for_user_data",
    filepath="/opt/airflow/data/aib-14/data.csv",
    mode= 'reschedule',
    dag=dag
)

get_user_names = PythonOperator(
    task_id="get_user_names",
    python_callable=_get_user_names,
    op_kwargs={
        "aib_num": "14"
    },
    dag=dag
)

create_user_names = PythonOperator(
    task_id="create_user_names",
    python_callable=_create_user_pages,
    templates_dict = {
        "aib_num" : "14"
    },
    dag=dag
)

wait_for_user_data >> get_user_names >> create_user_names