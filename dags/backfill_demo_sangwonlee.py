"""Q7 — catchup=True 로 start_date 부터 지금까지의 과거 구간을 자동으로 채우는(backfill) DAG.

각 실행은 자신의 logical date(ds) 가 들어간 파일에 결과를 쓴다 → 실행마다 산출물이 분리된다.
산출 위치는 컨테이너 안 /opt/airflow/backfill_output/
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

OUTPUT_DIR = "/opt/airflow/backfill_output"


def write_daily_file(ds: str) -> str:
    # ds: 이 실행의 logical date (YYYY-MM-DD) — Airflow 가 인자 이름을 보고 넣어 준다
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"daily_{ds}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"logical_date={ds}\n")
    print(f"wrote {path}")
    return path


def verify_file(ti, ds: str) -> None:
    path = ti.xcom_pull(task_ids="write_daily_file")
    with open(path, encoding="utf-8") as f:
        content = f.read().strip()
    print(f"{path}: {content}")
    if content != f"logical_date={ds}":
        raise ValueError(f"unexpected content for {ds}: {content!r}")


with DAG(
    dag_id="backfill_demo_sangwonlee",
    schedule="@daily",  # 매일 1회 (00:00 UTC)
    start_date=datetime(2026, 11, 3),  # 작업일(2026-11-10) 기준 7일 전, 고정 리터럴
    catchup=True,  # start_date ~ 지금 사이의 밀린 구간을 scheduled 실행으로 자동 생성
    tags=["week7", "q7"],
) as dag:
    write = PythonOperator(task_id="write_daily_file", python_callable=write_daily_file)
    verify = PythonOperator(task_id="verify_file", python_callable=verify_file)

    write >> verify
