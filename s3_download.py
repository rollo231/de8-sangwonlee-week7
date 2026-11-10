"""Q8 — boto3 로 S3 bronze/ 를 읽어 오는 스크립트.

한 흐름으로 순서대로 수행한다.
  [1] bronze/ 아래 객체 목록과 각 객체의 크기 출력
  [2] bronze/netflix_titles.csv 를 project/data/ 아래로 다운로드
  [3] 내려받은 파일의 행 수 출력 — CSV 레코드 기준
      (헤더 제외, 따옴표로 묶인 값 안의 줄바꿈은 한 행으로 센다 → 단순 줄 수와 다르다)

자격증명은 코드에 없다. boto3 기본 자격증명 체인(AWS_PROFILE 등)을 쓴다.
버킷 이름은 실행 인자(--bucket) 또는 환경변수(S3_BUCKET)로 받는다.

실행 예:
  AWS_PROFILE=week7 S3_BUCKET=de-8-sangwonlee python s3_download.py
"""

import argparse
import csv
import os
import sys

import boto3

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="S3 bronze/ 목록 → 다운로드 → CSV 레코드 수")
    p.add_argument("--bucket", default=os.environ.get("S3_BUCKET"), help="버킷 이름 (기본: $S3_BUCKET)")
    p.add_argument("--prefix", default="bronze/")
    p.add_argument("--key", default="bronze/netflix_titles.csv")
    p.add_argument("--dest-dir", default=os.path.join(PROJECT_DIR, "data"))
    args = p.parse_args()
    if not args.bucket:
        sys.exit("버킷 이름이 없습니다: --bucket 인자나 S3_BUCKET 환경변수를 지정하세요.")
    return args


def count_csv_records(path: str) -> int:
    # csv 모듈이 따옴표 규칙을 해석하므로, 값 안의 줄바꿈이 있어도 레코드 1개로 센다
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)  # 헤더 제외
        return sum(1 for _ in reader)


def main() -> None:
    args = parse_args()
    s3 = boto3.client("s3")

    # [1] 목록 + 크기
    print(f"[1] list     s3://{args.bucket}/{args.prefix}")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
        for obj in page.get("Contents", []):
            print(f"    {obj['Key']:<45} {obj['Size']:>12,} bytes")

    # [2] 다운로드
    os.makedirs(args.dest_dir, exist_ok=True)
    local_path = os.path.join(args.dest_dir, os.path.basename(args.key))
    s3.download_file(args.bucket, args.key, local_path)
    rel = os.path.relpath(local_path, PROJECT_DIR)
    print(f"[2] download s3://{args.bucket}/{args.key} -> {rel} ({os.path.getsize(local_path):,} bytes)")

    # [3] CSV 레코드 수
    print(f"[3] rows     {count_csv_records(local_path):,}  (CSV records, header excluded)")


if __name__ == "__main__":
    main()
