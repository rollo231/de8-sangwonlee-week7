"""Q4 — 텍스트 파일의 단어 빈도를 Spark Standalone 클러스터에서 계산한다.

흐름: 파일 읽기 → 공백 기준 분리 → 단어별 개수 집계 → 빈도 내림차순 상위 20개 show()
분리 규칙은 공백뿐 — 소문자 변환·구두점 제거를 하지 않는다 (Lorem ≠ lorem, amet ≠ amet,).

제출 (master 컨테이너 안):
  spark-submit --master spark://spark-master:7077 /opt/spark/jobs/wordcount.py
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/wordcount.txt"

spark = SparkSession.builder.appName("WordCount_sangwonlee").getOrCreate()

# 1) 파일 읽기 — 한 줄이 한 행(value 컬럼)
lines = spark.read.text(INPUT_PATH)

# 2) 공백 기준 분리 — 연속 공백·탭도 하나의 구분자로 보고, 빈 줄에서 생기는 "" 는 버린다
words = lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word")).filter(
    F.col("word") != ""
)

# 3) 단어별 개수 집계 — 동률은 단어 사전순으로 정렬해 실행마다 같은 결과가 나오게 한다
counts = words.groupBy("word").count().orderBy(F.desc("count"), F.asc("word"))

total_words = words.count()
distinct_words = counts.count()
print(f"total_words={total_words}  distinct_words={distinct_words}")

# 4) 빈도 내림차순 상위 20개
counts.show(20, truncate=False)

spark.stop()
