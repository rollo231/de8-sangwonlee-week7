"""Q3 — 시작 → 파이썬 작업 2개 → 종료, 4개 task 를 순서대로 잇는 기본 DAG."""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def say_hello() -> str:
    # print 가 아니라 return — 반환값은 Task Log 에 찍히고 XCom 에 저장된다.
    return "Hello Airflow"


def say_goodbye() -> str:
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    schedule="@daily",  # 매일 1회 (자정 UTC)
    start_date=datetime(2026, 11, 1),
    catchup=False,  # start_date ~ 오늘 사이의 과거 구간은 자동 실행하지 않음
    tags=["week7", "q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    hello_task = PythonOperator(task_id="hello_task", python_callable=say_hello)
    goodbye_task = PythonOperator(task_id="goodbye_task", python_callable=say_goodbye)
    end = EmptyOperator(task_id="end")

    start >> hello_task >> goodbye_task >> end
