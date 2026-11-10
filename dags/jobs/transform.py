"""Q9 — netflix_titles.csv 를 type × 장르별 작품 수로 집계해 parquet(snappy) 로 저장하는 Spark 잡.

처리:
  1) release_year >= --min-year (기본 2015) 인 행만 남긴다
  2) listed_in(쉼표로 구분된 장르 목록)을 장르 하나당 한 행으로 펼치고 앞뒤 공백을 제거한다
  3) type × genre 별 작품 수(title_count)를 집계한다
  4) 결과를 parquet + snappy 로 --output 경로에 저장하고, 집계 행 수를 출력한다

실행: spark-submit transform.py --input <csv> --output <dir> [--min-year 2015]
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True, help="netflix_titles.csv 로컬 경로")
parser.add_argument("--output", required=True, help="parquet 출력 디렉터리")
parser.add_argument("--min-year", type=int, default=2015, help="기준 연도 (이 연도 이상만 남김)")
args = parser.parse_args()

spark = SparkSession.builder.appName("netflix_transform_sangwonlee").getOrCreate()

# description 등 값 안에 줄바꿈·따옴표가 있으므로 multiLine + escape 를 켜야 레코드가 깨지지 않는다
df = spark.read.csv(args.input, header=True, multiLine=True, escape='"')
print(f"input rows = {df.count()}")

filtered = df.filter(F.col("release_year").cast("int") >= args.min_year)
print(f"rows with release_year >= {args.min_year} = {filtered.count()}")

genres = (
    filtered.withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
    .withColumn("genre", F.trim(F.col("genre")))
    .filter(F.col("genre") != "")
)

result = (
    genres.groupBy("type", "genre")
    .agg(F.count("*").alias("title_count"))
    .orderBy(F.col("type"), F.desc("title_count"), F.col("genre"))
)

n_rows = result.count()
result.show(n_rows, truncate=False)
print(f"aggregated rows = {n_rows}")

result.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(args.output)
print(f"saved parquet(snappy) -> {args.output}")

spark.stop()
