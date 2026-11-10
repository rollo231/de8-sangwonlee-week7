"""Q9 — S3(bronze) 다운로드 → Spark 집계 → S3(silver) 업로드를 세 개의 task 로 잇는 파이프라인.

- 자격증명: Airflow Connection aws_default (코드·설정에 키 없음)
- 버킷 이름: Airflow Variable s3_bucket (Q8 에서 만든 버킷)
- 기준 연도: DAG params.min_year (기본 2015) → transform.py 의 --min-year 인자로 주입
- 작업 파일은 컨테이너 안 /opt/airflow/pipeline/{날짜}/ 에 둔다 (호스트 파일을 직접 열지 않음)
"""

import glob
import os
from datetime import datetime

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, Variable

AWS_CONN_ID = "aws_default"
BRONZE_KEY = "bronze/netflix_titles.csv"
WORK_ROOT = "/opt/airflow/pipeline"


def download_csv(run_id: str) -> dict:
    bucket = Variable.get("s3_bucket")
    run_date = pendulum.now("Asia/Seoul").to_date_string()  # silver/{오늘날짜}/ 에 쓸 날짜
    # 실행마다 작업 폴더를 분리한다 — 같은 폴더를 두 실행이 동시에 overwrite 하면 Spark 쓰기가 깨진다
    work_dir = os.path.join(WORK_ROOT, run_date, run_id.replace(":", "-").replace("+", "_"))
    os.makedirs(work_dir, exist_ok=True)

    local_path = os.path.join(work_dir, "netflix_titles.csv")
    S3Hook(aws_conn_id=AWS_CONN_ID).get_conn().download_file(bucket, BRONZE_KEY, local_path)
    print(f"downloaded s3://{bucket}/{BRONZE_KEY} -> {local_path} ({os.path.getsize(local_path):,} bytes)")

    # 다음 task 들이 쓸 경로를 XCom 으로 넘긴다 (데이터 자체가 아니라 경로만)
    return {"run_date": run_date, "input": local_path, "output": os.path.join(work_dir, "silver")}


def upload_silver(ti) -> int:
    info = ti.xcom_pull(task_ids="download_csv")
    bucket = Variable.get("s3_bucket")
    prefix = f"silver/{info['run_date']}/"
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    files = sorted(glob.glob(os.path.join(info["output"], "*.parquet")))
    if not files:
        raise FileNotFoundError(f"no parquet files in {info['output']}")

    # 같은 날 다시 실행해도 결과가 한 벌만 남도록(멱등) 기존 객체를 먼저 지운다
    old_keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    if old_keys:
        hook.delete_objects(bucket=bucket, keys=old_keys)
        print(f"removed {len(old_keys)} old object(s) under s3://{bucket}/{prefix}")

    for path in files:
        key = prefix + os.path.basename(path)
        hook.load_file(filename=path, key=key, bucket_name=bucket, replace=True)
        print(f"uploaded {path} -> s3://{bucket}/{key}")

    resp = hook.get_conn().list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = resp.get("Contents", [])
    print(f"objects under s3://{bucket}/{prefix}:")
    for obj in objects:
        print(f"  {obj['Key']}  {obj['Size']:,} bytes")
    print(f"object count = {len(objects)}")
    return len(objects)


with DAG(
    dag_id="weekly_pipeline_sangwonlee",
    schedule="@weekly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,  # 같은 silver/{날짜}/ 에 쓰므로 한 번에 한 실행만
    params={"min_year": Param(2015, type="integer", description="이 연도 이상만 집계")},
    tags=["week7", "q9"],
) as dag:
    download = PythonOperator(task_id="download_csv", python_callable=download_csv)

    transform = SparkSubmitOperator(
        task_id="transform",
        conn_id="spark_local",  # host=local[*] — Airflow 워커 안에서 Spark 실행
        application="/opt/airflow/dags/jobs/transform.py",
        application_args=[
            "--input", "{{ ti.xcom_pull(task_ids='download_csv')['input'] }}",
            "--output", "{{ ti.xcom_pull(task_ids='download_csv')['output'] }}",
            "--min-year", "{{ params.min_year }}",
        ],
        name="netflix_transform_sangwonlee",
    )

    upload = PythonOperator(task_id="upload_silver", python_callable=upload_silver)

    download >> transform >> upload
