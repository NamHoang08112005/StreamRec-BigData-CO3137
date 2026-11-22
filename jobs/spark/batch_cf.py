"""
Batch layer (Lambda): ALS collaborative filtering on interactions -> cf_scores.
Runs on Spark cluster (spark://spark-master:7077) and records completion metadata to Redis.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def _report_redis(status: dict) -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    try:
        import redis

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        payload = {
            **status,
            "engine": "spark_als",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "interval_sec": int(os.getenv("SPARK_BATCH_INTERVAL_SEC", "180") or 180),
        }
        r.set("metrics:batch_cf:last", json.dumps(payload, separators=(",", ":")))
    except Exception as exc:  # noqa: BLE001
        print(f"Redis status update skipped: {exc}")


def main() -> None:
    jdbc_url = os.getenv("DATABASE_URL", "jdbc:postgresql://postgres:5432/recsys")
    user = os.getenv("PG_USER", "recsys")
    password = os.getenv("PG_PASSWORD", "recsys")

    spark = (
        SparkSession.builder.appName("recsys-batch-als")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    props = {"user": user, "password": password, "driver": "org.postgresql.Driver"}
    interactions = spark.read.jdbc(jdbc_url, "interactions", properties=props).select("user_id", "video_id", "weight")

    agg_ratings = (
        interactions.groupBy("user_id", "video_id")
        .agg(F.sum("weight").alias("rating"))
        .filter(F.col("rating") > 0)
        .cache()
    )

    n_ratings = agg_ratings.count()
    if n_ratings == 0:
        print("No positive interactions; skip ALS")
        _report_redis({"status": "empty", "pairs": 0, "users": 0, "videos": 0})
        spark.stop()
        return

    user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx_d").fit(agg_ratings)
    video_indexer = StringIndexer(inputCol="video_id", outputCol="video_idx_d").fit(agg_ratings)
    indexed = (
        video_indexer.transform(user_indexer.transform(agg_ratings))
        .withColumn("user_idx", F.col("user_idx_d").cast("int"))
        .withColumn("video_idx", F.col("video_idx_d").cast("int"))
    )

    users = indexed.select("user_idx", "user_id").distinct().cache()
    videos = indexed.select("video_idx", "video_id").distinct().cache()
    seen_pairs = indexed.select("user_idx", "video_idx").distinct()

    als = ALS(
        userCol="user_idx",
        itemCol="video_idx",
        ratingCol="rating",
        nonnegative=True,
        coldStartStrategy="drop",
        rank=16,
        maxIter=8,
        regParam=0.1,
    )
    model = als.fit(indexed)
    recs = model.recommendForAllUsers(80)
    exploded = recs.select(
        F.col("user_idx"),
        F.explode("recommendations").alias("rec"),
    ).select(
        "user_idx",
        F.col("rec.video_idx").alias("video_idx"),
        F.col("rec.rating").cast("double").alias("raw_score"),
    )

    # Exclude already-interacted items so Bloom Filter doesn't drop all CF hits
    unseen = exploded.join(seen_pairs, on=["user_idx", "video_idx"], how="left_anti")
    w_user = Window.partitionBy("user_idx").orderBy(F.col("raw_score").desc())
    w_max = Window.partitionBy("user_idx")
    top_unseen = (
        unseen.withColumn("rn", F.row_number().over(w_user))
        .filter(F.col("rn") <= 50)
        .withColumn("max_s", F.max("raw_score").over(w_max))
        .withColumn("score", F.when(F.col("max_s") > 0, F.col("raw_score") / F.col("max_s")).otherwise(F.lit(0.0)))
    )

    out = (
        top_unseen.join(users, "user_idx")
        .join(videos, "video_idx")
        .select("user_id", "video_id", "score")
        .withColumn("updated_at", F.current_timestamp())
        .cache()
    )

    pairs = out.count()
    n_users = users.count()
    n_videos = videos.count()
    (
        out.write.mode("overwrite")
        .option("truncate", "true")
        .jdbc(jdbc_url, "cf_scores", properties=props)
    )
    print(f"Wrote cf_scores rows={pairs} users={n_users} videos={n_videos}")
    _report_redis({"status": "ok", "pairs": pairs, "users": n_users, "videos": n_videos})
    spark.stop()


if __name__ == "__main__":
    main()
