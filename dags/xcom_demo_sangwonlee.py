"""Q6 — XCom 으로 앞 task 의 반환값을 뒤 task 가 받아 쓰고, 앞 task 는 재시도로 성공한다.

- count_lines: 이 DAG 파일의 줄 수를 세어 return → XCom 에 key=return_value 로 저장
- use_value  : xcom_pull 로 그 값을 받아 로그에 출력
- count_lines 는 retries=2. 첫 시도(try_number=1)에서만 일부러 실패 → 두 번째 시도에서 성공
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def count_lines(ti) -> int:
    # ti(TaskInstance)는 Airflow 가 함수 인자 이름을 보고 자동으로 넣어 준다
    print(f"try_number = {ti.try_number}")
    if ti.try_number == 1:
        raise RuntimeError("첫 시도는 일부러 실패시킨다 — retries 로 재시도되는지 확인")

    with open(__file__, encoding="utf-8") as f:
        n_lines = sum(1 for _ in f)
    print(f"{__file__} has {n_lines} lines")
    return n_lines  # return 값은 XCom(key=return_value)에 저장된다


def use_value(ti) -> None:
    value = ti.xcom_pull(task_ids="count_lines")  # key 기본값 = return_value
    print(f"received from XCom: {value}")
    print(f"doubled = {value * 2}")


with DAG(
    dag_id="xcom_demo_sangwonlee",
    schedule=None,  # 수동 트리거
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["week7", "q6"],
) as dag:
    t1 = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    t2 = PythonOperator(task_id="use_value", python_callable=use_value)

    t1 >> t2
